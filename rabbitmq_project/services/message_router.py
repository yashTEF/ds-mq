"""
Message Router Service

Provides high-level routing functionality to connect producers, exchanges,
queues, and bindings in a coordinated way.
"""
import logging
import json
from typing import Dict, List, Any, Optional, Union, Callable
import pika

from ..core.connection_manager import get_connection, get_channel
from ..exchanges.exchange_manager import ExchangeManager
from ..queues.queue_manager import QueueManager
from ..bindings.binding_manager import BindingManager
from ..core.retry_handler import retry_handler

# Configure logging
logger = logging.getLogger(__name__)

class MessageRouter:
    """
    A high-level facade for routing messages between system components.
    Coordinates exchanges, queues, and bindings to simplify message routing.
    """
    
    def __init__(self):
        """Initialize the message router with required components."""
        self.exchange_manager = ExchangeManager()
        self.queue_manager = QueueManager()
        self.binding_manager = BindingManager()
        self._declare_default_infrastructure()
        
    def _declare_default_infrastructure(self):
        """Declare default exchanges and queues needed by the system."""
        try:
            # Declare the default direct exchange for simple routing
            self.exchange_manager.declare_exchange(
                exchange_name='default_direct',
                exchange_type='direct',
                durable=True
            )
            
            # Declare the default topic exchange for pattern-based routing
            self.exchange_manager.declare_exchange(
                exchange_name='default_topic',
                exchange_type='topic',
                durable=True
            )
            
            # Declare the default fanout exchange for broadcast messaging
            self.exchange_manager.declare_exchange(
                exchange_name='default_fanout',
                exchange_type='fanout',
                durable=True
            )
            
            # Declare the default headers exchange for attribute-based routing
            self.exchange_manager.declare_exchange(
                exchange_name='default_headers',
                exchange_type='headers',
                durable=True
            )
            
            # Declare a notifications exchange
            self.exchange_manager.declare_exchange(
                exchange_name='notifications',
                exchange_type='topic',
                durable=True
            )
            
            logger.info("Default messaging infrastructure declared successfully")
        except Exception as e:
            logger.error(f"Failed to declare default infrastructure: {e}")
            
    def create_route(
        self, 
        exchange_name: str,
        exchange_type: str,
        queue_name: str,
        routing_key: str = '',
        headers: Dict[str, Any] = None,
        queue_arguments: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Create a complete route from exchange to queue with appropriate binding.
        
        Args:
            exchange_name: Name of the exchange
            exchange_type: Type of exchange ('direct', 'topic', 'fanout', 'headers')
            queue_name: Name of the queue
            routing_key: Routing key for binding (not used for fanout)
            headers: Header bindings for headers exchange
            queue_arguments: Additional arguments for queue declaration
            
        Returns:
            Dict with exchange, queue, and binding information
        """
        try:
            # Declare the exchange
            exchange = self.exchange_manager.declare_exchange(
                exchange_name=exchange_name,
                exchange_type=exchange_type,
                durable=True
            )
            
            # Declare the queue
            queue = self.queue_manager.declare_queue(
                queue_name=queue_name,
                durable=True,
                arguments=queue_arguments
            )
            
            # Create the binding
            binding = None
            if exchange_type == 'headers':
                binding = self.binding_manager.bind_queue_with_headers(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    headers=headers or {}
                )
            else:
                binding = self.binding_manager.bind_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=routing_key
                )
            
            logger.info(f"Created route: {exchange_name} -> {queue_name} with key '{routing_key}'")
            
            return {
                'exchange': exchange,
                'queue': queue,
                'binding': binding
            }
        except Exception as e:
            logger.error(f"Failed to create route {exchange_name} -> {queue_name}: {e}")
            raise
            
    def create_dead_letter_route(
        self,
        source_queue_name: str,
        dead_letter_exchange: str = 'dead-letter-exchange',
        dead_letter_queue: str = 'dead-letter-queue',
        ttl: int = 1000 * 60 * 60 * 24  # 1 day default
    ) -> Dict[str, Any]:
        """
        Create a dead letter route for a queue.
        
        Args:
            source_queue_name: Name of the source queue
            dead_letter_exchange: Name of the dead letter exchange
            dead_letter_queue: Name of the dead letter queue
            ttl: Time-to-live for messages in milliseconds
            
        Returns:
            Dict with exchange, queue, and binding information
        """
        try:
            # Declare the dead letter exchange
            dlx = self.exchange_manager.declare_exchange(
                exchange_name=dead_letter_exchange,
                exchange_type='direct',
                durable=True
            )
            
            # Declare the dead letter queue with TTL
            dlq = self.queue_manager.declare_queue(
                queue_name=dead_letter_queue,
                durable=True,
                arguments={'x-message-ttl': ttl}
            )
            
            # Bind the dead letter queue to the exchange
            binding = self.binding_manager.bind_queue(
                queue_name=dead_letter_queue,
                exchange_name=dead_letter_exchange,
                routing_key='#'  # Catch all routing keys
            )
            
            # Update the source queue to use this dead letter exchange
            source_queue = self.queue_manager.declare_queue(
                queue_name=source_queue_name,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': dead_letter_exchange,
                    'x-dead-letter-routing-key': source_queue_name
                }
            )
            
            logger.info(f"Created dead letter route for queue {source_queue_name}")
            
            return {
                'exchange': dlx,
                'queue': dlq,
                'binding': binding,
                'source_queue': source_queue
            }
        except Exception as e:
            logger.error(f"Failed to create dead letter route for {source_queue_name}: {e}")
            raise
            
    def publish_message(
        self, 
        exchange_name: str, 
        routing_key: str,
        message: Union[Dict[str, Any], str],
        content_type: str = 'application/json',
        headers: Dict[str, Any] = None,
        correlation_id: str = None,
        reply_to: str = None,
        expiration: str = None,
        priority: int = None,
        delivery_mode: int = 2  # 2 = persistent
    ) -> bool:
        """
        Publish a message to the specified exchange.
        
        Args:
            exchange_name: Name of the exchange
            routing_key: Routing key for the message
            message: Message content (will be JSON serialized if dict)
            content_type: Content type of the message
            headers: Optional headers for the message
            correlation_id: Optional correlation ID
            reply_to: Optional queue to reply to
            expiration: Optional expiration time in milliseconds
            priority: Optional priority (0-9)
            delivery_mode: 1 = non-persistent, 2 = persistent
            
        Returns:
            True if successful, False otherwise
        """
        try:
            channel = get_channel()
            
            # Prepare message body
            if isinstance(message, dict):
                body = json.dumps(message).encode('utf-8')
            elif isinstance(message, str):
                body = message.encode('utf-8')
            else:
                body = str(message).encode('utf-8')
            
            # Prepare message properties
            properties = pika.BasicProperties(
                content_type=content_type,
                headers=headers or {},
                delivery_mode=delivery_mode,
                correlation_id=correlation_id,
                reply_to=reply_to,
                expiration=expiration,
                priority=priority
            )
            
            # Publish the message
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=body,
                properties=properties
            )
            
            logger.debug(f"Published message to {exchange_name} with key {routing_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
            
    def register_consumer(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = False,
        prefetch_count: int = 1,
        consumer_name: str = '',
        arguments: Dict[str, Any] = None
    ) -> str:
        """
        Register a consumer for a queue.
        
        Args:
            queue_name: Name of the queue to consume from
            callback: Callback function for message processing
            auto_ack: Whether to auto-acknowledge messages
            prefetch_count: Number of unacknowledged messages to prefetch
            consumer_name: Consumer identifier
            arguments: Additional arguments for basic_consume
            
        Returns:
            Consumer tag
        """
        try:
            channel = get_channel()
            
            # Set QoS prefetch count
            channel.basic_qos(prefetch_count=prefetch_count)
            
            # Start consuming
            consumer_tag = channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack,
                consumer_tag=consumer_name or None,
                arguments=arguments
            )
            
            logger.info(f"Registered consumer for queue {queue_name} with tag {consumer_tag}")
            return consumer_tag
            
        except Exception as e:
            logger.error(f"Failed to register consumer for queue {queue_name}: {e}")
            raise
            
    def handle_message_failure(
        self,
        channel,
        delivery_tag: int,
        properties: pika.BasicProperties,
        body: bytes,
        queue_name: str,
        exception: Exception = None
    ):
        """
        Handle a message processing failure with retry logic.
        
        Args:
            channel: Pika channel
            delivery_tag: Message delivery tag
            properties: Message properties
            body: Message body
            queue_name: Name of the queue
            exception: Exception that caused the failure
        """
        retry_handler.process_retry(
            channel,
            delivery_tag=delivery_tag,
            properties=properties,
            body=body,
            queue_name=queue_name,
            exception=exception
        )
        
    def create_request_reply_pattern(
        self,
        request_exchange: str,
        request_key: str,
        reply_queue: str = None,
        ttl: int = 30000,  # 30 seconds
        durable: bool = False
    ) -> Dict[str, Any]:
        """
        Create a request-reply messaging pattern.
        
        Args:
            request_exchange: Exchange for requests
            request_key: Routing key for requests
            reply_queue: Name of reply queue (auto-generated if None)
            ttl: Time-to-live for reply queue in milliseconds
            durable: Whether the reply queue should be durable
            
        Returns:
            Dict with exchange and queue information
        """
        channel = get_channel()
        
        # Declare the request exchange if it doesn't exist
        self.exchange_manager.declare_exchange(
            exchange_name=request_exchange,
            exchange_type='direct',
            durable=True
        )
        
        # Create exclusive reply queue if not specified
        if not reply_queue:
            result = channel.queue_declare(
                queue='',  # Let RabbitMQ generate a name
                exclusive=True,
                auto_delete=True,
                arguments={'x-message-ttl': ttl}
            )
            reply_queue = result.method.queue
        else:
            # Declare the specified reply queue
            channel.queue_declare(
                queue=reply_queue,
                durable=durable,
                arguments={'x-message-ttl': ttl}
            )
        
        logger.info(f"Created request-reply pattern with reply queue {reply_queue}")
        
        return {
            'request_exchange': request_exchange,
            'request_key': request_key,
            'reply_queue': reply_queue
        }
        
    def create_work_queue_pattern(
        self,
        queue_name: str,
        durable: bool = True,
        prefetch_count: int = 1
    ) -> Dict[str, Any]:
        """
        Create a work queue pattern for fair task distribution.
        
        Args:
            queue_name: Name of the work queue
            durable: Whether the queue should survive restarts
            prefetch_count: Number of tasks to prefetch per worker
            
        Returns:
            Dict with queue information
        """
        channel = get_channel()
        
        # Declare a durable queue
        queue = self.queue_manager.declare_queue(
            queue_name=queue_name,
            durable=durable
        )
        
        # Set QoS prefetch count
        channel.basic_qos(prefetch_count=prefetch_count)
        
        logger.info(f"Created work queue pattern with queue {queue_name}")
        
        return {
            'queue': queue,
            'prefetch_count': prefetch_count
        }
        
    def create_pub_sub_pattern(
        self,
        exchange_name: str,
        queue_prefix: str = 'pubsub_',
        num_subscribers: int = 1,
        auto_delete: bool = True
    ) -> Dict[str, Any]:
        """
        Create a publish-subscribe pattern with fanout exchange.
        
        Args:
            exchange_name: Name of the fanout exchange
            queue_prefix: Prefix for subscriber queue names
            num_subscribers: Number of subscriber queues to create
            auto_delete: Whether queues should auto-delete when unused
            
        Returns:
            Dict with exchange and queues information
        """
        # Declare the fanout exchange
        exchange = self.exchange_manager.declare_exchange(
            exchange_name=exchange_name,
            exchange_type='fanout',
            durable=True
        )
        
        queues = []
        
        # Create subscriber queues
        for i in range(num_subscribers):
            queue_name = f"{queue_prefix}{i+1}"
            queue = self.queue_manager.declare_queue(
                queue_name=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=auto_delete
            )
            
            # Bind queue to fanout exchange (no routing key needed)
            self.binding_manager.bind_queue(
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_key=''
            )
            
            queues.append(queue)
            
        logger.info(f"Created pub-sub pattern with exchange {exchange_name} and {num_subscribers} queues")
        
        return {
            'exchange': exchange,
            'queues': queues
        }

# Create a singleton instance
message_router = MessageRouter()