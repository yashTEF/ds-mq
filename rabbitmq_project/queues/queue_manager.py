"""
Queue manager for declaring and managing RabbitMQ queues.
Supports standard, durable, and quorum queues with various options.
"""
from ..core.channel_manager import get_channel
from ..core.logger import get_logger
from ..config.config import config

# Module logger
logger = get_logger(__name__)

class QueueManager:
    """
    Manages RabbitMQ queues.
    Provides methods to declare, bind, and delete queues with various options.
    """
    
    def __init__(self):
        """Initialize queue manager."""
        pass
        
    def declare_queue(self, queue_name, durable=None, exclusive=False,
                      auto_delete=False, arguments=None, channel=None):
        """
        Declare a RabbitMQ queue.
        
        Args:
            queue_name: Name of the queue
            durable: Whether the queue survives broker restarts
            exclusive: Whether the queue can only be used by the declaring connection
            auto_delete: Whether to delete the queue when no consumers are subscribed
            arguments: Additional queue arguments
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        if durable is None:
            durable = config.DEFAULT_QUEUE_DURABLE
            
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Declaring queue '{queue_name}' (durable={durable})")
            
            result = channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments
            )
            
            logger.debug(f"Queue '{queue_name}' declared successfully with {result.method.message_count} messages")
            return result
            
        except Exception as e:
            logger.error(f"Failed to declare queue '{queue_name}': {e}")
            raise
            
    def delete_queue(self, queue_name, if_unused=False, if_empty=False, channel=None):
        """
        Delete a RabbitMQ queue.
        
        Args:
            queue_name: Name of the queue to delete
            if_unused: Only delete if the queue has no consumers
            if_empty: Only delete if the queue is empty
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if queue deleted successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Deleting queue '{queue_name}'")
            
            channel.queue_delete(
                queue=queue_name,
                if_unused=if_unused,
                if_empty=if_empty
            )
            
            logger.debug(f"Queue '{queue_name}' deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete queue '{queue_name}': {e}")
            raise
            
    def purge_queue(self, queue_name, channel=None):
        """
        Purge all messages from a queue.
        
        Args:
            queue_name: Name of the queue to purge
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if queue purged successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Purging queue '{queue_name}'")
            
            channel.queue_purge(queue=queue_name)
            
            logger.debug(f"Queue '{queue_name}' purged successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to purge queue '{queue_name}': {e}")
            raise
            
    def declare_durable_queue(self, queue_name, channel=None):
        """
        Declare a durable queue that survives broker restarts.
        
        Args:
            queue_name: Name of the queue
            channel: Optional channel to use
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        return self.declare_queue(
            queue_name,
            durable=True,
            channel=channel
        )
        
    def declare_quorum_queue(self, queue_name, channel=None):
        """
        Declare a quorum queue that provides higher availability.
        
        Args:
            queue_name: Name of the queue
            channel: Optional channel to use
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        arguments = {
            'x-queue-type': 'quorum',
            'x-delivery-limit': 10  # Maximum delivery attempts before dead-lettering
        }
        
        return self.declare_queue(
            queue_name,
            durable=True,  # Quorum queues are always durable
            arguments=arguments,
            channel=channel
        )
        
    def declare_temporary_queue(self, channel=None):
        """
        Declare a temporary exclusive queue with a server-generated name.
        
        Args:
            channel: Optional channel to use
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        return self.declare_queue(
            queue_name='',  # Let server generate a name
            durable=False,
            exclusive=True,
            auto_delete=True,
            channel=channel
        )
        
    def declare_queue_with_ttl(self, queue_name, ttl_ms, channel=None):
        """
        Declare a queue with message TTL (time-to-live).
        
        Args:
            queue_name: Name of the queue
            ttl_ms: Message TTL in milliseconds
            channel: Optional channel to use
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        arguments = {
            'x-message-ttl': ttl_ms
        }
        
        return self.declare_queue(
            queue_name,
            durable=True,
            arguments=arguments,
            channel=channel
        )
        
    def declare_queue_with_dlx(self, queue_name, dead_letter_exchange, 
                              dead_letter_routing_key=None, channel=None):
        """
        Declare a queue with a dead-letter exchange for failed messages.
        
        Args:
            queue_name: Name of the queue
            dead_letter_exchange: Name of the dead-letter exchange
            dead_letter_routing_key: Optional routing key for dead-lettered messages
            channel: Optional channel to use
            
        Returns:
            pika.frame.Method with the queue declaration result
        """
        arguments = {
            'x-dead-letter-exchange': dead_letter_exchange
        }
        
        if dead_letter_routing_key:
            arguments['x-dead-letter-routing-key'] = dead_letter_routing_key
            
        return self.declare_queue(
            queue_name,
            durable=True,
            arguments=arguments,
            channel=channel
        )
        
    def get_queue_message_count(self, queue_name, channel=None):
        """
        Get the number of messages in a queue.
        
        Args:
            queue_name: Name of the queue
            channel: Optional channel to use
            
        Returns:
            The number of messages in the queue
        """
        try:
            if channel is None:
                channel = get_channel()
                
            result = channel.queue_declare(queue=queue_name, passive=True)
            count = result.method.message_count
            logger.debug(f"Queue '{queue_name}' has {count} messages")
            return count
            
        except Exception as e:
            logger.error(f"Failed to get message count for queue '{queue_name}': {e}")
            raise
            
    def get_queue_consumer_count(self, queue_name, channel=None):
        """
        Get the number of consumers for a queue.
        
        Args:
            queue_name: Name of the queue
            channel: Optional channel to use
            
        Returns:
            The number of consumers for the queue
        """
        try:
            if channel is None:
                channel = get_channel()
                
            result = channel.queue_declare(queue=queue_name, passive=True)
            count = result.method.consumer_count
            logger.debug(f"Queue '{queue_name}' has {count} consumers")
            return count
            
        except Exception as e:
            logger.error(f"Failed to get consumer count for queue '{queue_name}': {e}")
            raise

# Singleton instance
queue_manager = QueueManager()