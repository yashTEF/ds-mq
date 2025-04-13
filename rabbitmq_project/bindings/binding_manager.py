"""
Binding manager for creating and managing bindings between exchanges and queues.
Supports direct, topic, fanout, and headers exchange bindings.
"""
from ..core.channel_manager import get_channel
from ..core.logger import get_logger

# Module logger
logger = get_logger(__name__)

class BindingManager:
    """
    Manages RabbitMQ bindings between exchanges and queues.
    Provides methods to create and delete bindings with various options.
    """
    
    def __init__(self):
        """Initialize binding manager."""
        pass
        
    def bind_queue(self, queue_name, exchange_name, routing_key='', 
                  arguments=None, channel=None):
        """
        Bind a queue to an exchange.
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the exchange to bind to
            routing_key: Routing key for the binding
            arguments: Additional binding arguments
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if binding created successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Binding queue '{queue_name}' to exchange '{exchange_name}' "
                       f"with routing key '{routing_key}'")
            
            channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key,
                arguments=arguments
            )
            
            logger.debug(f"Queue '{queue_name}' bound to exchange '{exchange_name}' successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to bind queue '{queue_name}' to exchange '{exchange_name}': {e}")
            raise
            
    def unbind_queue(self, queue_name, exchange_name, routing_key='',
                    arguments=None, channel=None):
        """
        Unbind a queue from an exchange.
        
        Args:
            queue_name: Name of the queue to unbind
            exchange_name: Name of the exchange to unbind from
            routing_key: Routing key for the binding
            arguments: Additional binding arguments
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if binding removed successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Unbinding queue '{queue_name}' from exchange '{exchange_name}' "
                       f"with routing key '{routing_key}'")
            
            channel.queue_unbind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key,
                arguments=arguments
            )
            
            logger.debug(f"Queue '{queue_name}' unbound from exchange '{exchange_name}' successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unbind queue '{queue_name}' from exchange '{exchange_name}': {e}")
            raise
            
    def bind_queue_direct(self, queue_name, exchange_name, routing_key, channel=None):
        """
        Bind a queue to a direct exchange with a specific routing key.
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the direct exchange to bind to
            routing_key: Exact routing key to match
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        return self.bind_queue(
            queue_name,
            exchange_name,
            routing_key=routing_key,
            channel=channel
        )
        
    def bind_queue_topic(self, queue_name, exchange_name, topic_pattern, channel=None):
        """
        Bind a queue to a topic exchange with a topic pattern.
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the topic exchange to bind to
            topic_pattern: Topic pattern with wildcards (* and #)
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        return self.bind_queue(
            queue_name,
            exchange_name,
            routing_key=topic_pattern,
            channel=channel
        )
        
    def bind_queue_fanout(self, queue_name, exchange_name, channel=None):
        """
        Bind a queue to a fanout exchange (ignores routing key).
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the fanout exchange to bind to
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        return self.bind_queue(
            queue_name,
            exchange_name,
            routing_key='',  # Ignored for fanout exchanges
            channel=channel
        )
        
    def bind_queue_headers(self, queue_name, exchange_name, header_match, 
                          match_all=True, channel=None):
        """
        Bind a queue to a headers exchange with header matching.
        
        Args:
            queue_name: Name of the queue to bind
            exchange_name: Name of the headers exchange to bind to
            header_match: Dictionary of headers to match
            match_all: If True, all headers must match (x-match=all), otherwise any match is sufficient
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        arguments = header_match.copy()
        arguments['x-match'] = 'all' if match_all else 'any'
        
        return self.bind_queue(
            queue_name,
            exchange_name,
            routing_key='',  # Ignored for headers exchanges
            arguments=arguments,
            channel=channel
        )
        
    def create_work_queue_binding(self, queue_name, exchange_name='', routing_key=None, channel=None):
        """
        Create a binding for a work queue pattern.
        For work queues, we typically use the default exchange or a direct exchange.
        
        Args:
            queue_name: Name of the work queue
            exchange_name: Name of the exchange (default is the nameless default exchange)
            routing_key: Routing key (defaults to queue name for default exchange)
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        # If using default exchange, routing key should be the queue name
        if exchange_name == '' and routing_key is None:
            routing_key = queue_name
            
        return self.bind_queue(
            queue_name,
            exchange_name,
            routing_key=routing_key or queue_name,
            channel=channel
        )
        
    def create_pubsub_binding(self, queue_name, exchange_name, channel=None):
        """
        Create a binding for a pub/sub pattern using a fanout exchange.
        
        Args:
            queue_name: Name of the subscriber queue
            exchange_name: Name of the fanout exchange
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        return self.bind_queue_fanout(
            queue_name,
            exchange_name,
            channel=channel
        )

# Singleton instance
binding_manager = BindingManager()