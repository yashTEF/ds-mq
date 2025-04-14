"""
Message Router Service

Provides high-level routing functionality to connect producers, exchanges,
queues, and bindings in a coordinated way. Supports various messaging patterns
including work queues, pub/sub, topic routing, and request-reply.
"""
import json
from typing import Dict, List, Any, Optional, Union, Callable
import pika

from ..core.connection_manager import get_connection, get_channel
from ..exchanges.exchange_manager import exchange_manager
from ..queues.queue_manager import queue_manager
from ..bindings.binding_manager import binding_manager
from ..core.retry_handler import retry_handler
from ..core.logger import get_logger

# Configure logging
logger = get_logger(__name__)

class MessageRouter:
    """
    A high-level facade for routing messages between system components.
    Coordinates exchanges, queues, and bindings to simplify message routing.
    """
    
    def __init__(self):
        """Initialize the message router with required components."""
        self._channel = None
        self._declare_default_infrastructure()
        
    def get_channel(self):
        """Get a channel for operations."""
        if self._channel is None:
            self._channel = get_channel()
        return self._channel
        
    def _declare_default_infrastructure(self):
        """Declare default exchanges and queues needed by the system."""
        try:
            channel = self.get_channel()
            
            # Declare the default direct exchange for simple routing
            exchange_manager.declare_exchange(
                exchange_name='default_direct',
                exchange_type='direct',
                durable=True,
                channel=channel
            )
            
            # Declare the default topic exchange for pattern-based routing
            exchange_manager.declare_exchange(
                exchange_name='default_topic',
                exchange_type='topic',
                durable=True,
                channel=channel
            )
            
            # Declare the default fanout exchange for broadcast messaging
            exchange_manager.declare_exchange(
                exchange_name='default_fanout',
                exchange_type='fanout',
                durable=True,
                channel=channel
            )
            
            # Declare the default headers exchange for attribute-based routing
            exchange_manager.declare_exchange(
                exchange_name='default_headers',
                exchange_type='headers',
                durable=True,
                channel=channel
            )
            
            logger.info("Default messaging infrastructure declared successfully")
        except Exception as e:
            logger.error(f"Failed to declare default infrastructure: {e}")
    
    # === Basic Work Queue Pattern ===
    
    def setup_work_queue(self, queue_name, durable=True, prefetch_count=1):
        """
        Set up a work queue pattern.
        In this pattern, messages are distributed among multiple workers.
        
        Args:
            queue_name: The name of the work queue
            durable: Whether the queue should survive broker restarts
            prefetch_count: Number of tasks to prefetch per worker
            
        Returns:
            Dict with queue information
        """
        channel = self.get_channel()
        
        # Declare a durable queue
        queue = queue_manager.declare_queue(
            queue_name=queue_name,
            durable=durable,
            channel=channel
        )
        
        # Set QoS prefetch count for fair dispatch
        channel.basic_qos(prefetch_count=prefetch_count)
        
        logger.info(f"Work queue '{queue_name}' set up successfully")
        
        return {
            'queue': queue,
            'prefetch_count': prefetch_count
        }
    
    # === Pub/Sub Pattern ===
    
    def setup_pubsub_pattern(self, exchange_name, queue_names=None, auto_delete_queues=False):
        """
        Set up a publish-subscribe pattern using a fanout exchange.
        In this pattern, messages are broadcast to all bound queues.
        
        Args:
            exchange_name: The name of the fanout exchange
            queue_names: Optional list of queue names to create and bind
            auto_delete_queues: Whether queues should be deleted when no longer used
            
        Returns:
            Dict with exchange and queues information
        """
        channel = self.get_channel()
        
        # Declare the fanout exchange
        exchange = exchange_manager.declare_fanout_exchange(
            exchange_name=exchange_name,
            channel=channel
        )
        
        queues = []
        
        # If queue names provided, create and bind them
        if queue_names:
            for queue_name in queue_names:
                # Declare the queue
                queue = queue_manager.declare_queue(
                    queue_name=queue_name,
                    durable=True,
                    auto_delete=auto_delete_queues,
                    channel=channel
                )
                
                # Bind the queue to the exchange (routing key ignored for fanout)
                binding_manager.bind_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key='',
                    channel=channel
                )
                
                queues.append(queue)
            
            logger.info(f"PubSub pattern set up with exchange '{exchange_name}' and {len(queues)} queues")
        else:
            logger.info(f"PubSub exchange '{exchange_name}' set up (no queues bound)")
            
        return {
            'exchange': exchange,
            'queues': queues
        }
    
    # === Topic Routing Pattern ===
    
    def setup_topic_routing(self, exchange_name, topic_bindings=None):
        """
        Set up a topic routing pattern.
        In this pattern, messages are routed based on wildcard matches.
        
        Args:
            exchange_name: The name of the topic exchange
            topic_bindings: Optional dict of {queue_name: topic_pattern} to create and bind
            
        Returns:
            Dict with exchange and bindings information
        """
        channel = self.get_channel()
        
        # Declare the topic exchange
        exchange = exchange_manager.declare_topic_exchange(
            exchange_name=exchange_name,
            channel=channel
        )
        
        bindings = []
        
        # If topic bindings provided, create queues and bind them
        if topic_bindings:
            for queue_name, topic_pattern in topic_bindings.items():
                # Declare the queue
                queue_manager.declare_queue(
                    queue_name=queue_name,
                    durable=True,
                    channel=channel
                )
                
                # Bind the queue with the topic pattern
                binding = binding_manager.bind_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=topic_pattern,
                    channel=channel
                )
                
                bindings.append({
                    'queue': queue_name,
                    'pattern': topic_pattern,
                    'binding': binding
                })
                
            logger.info(f"Topic routing set up with exchange '{exchange_name}' and {len(bindings)} queues")
        else:
            logger.info(f"Topic exchange '{exchange_name}' set up (no queues bound)")
            
        return {
            'exchange': exchange,
            'bindings': bindings
        }
    
    # === Direct Routing Pattern ===
    
    def setup_direct_routing(self, exchange_name, direct_bindings=None):
        """
        Set up a direct routing pattern.
        In this pattern, messages are routed based on exact routing key matches.
        
        Args:
            exchange_name: The name of the direct exchange
            direct_bindings: Optional dict of {queue_name: routing_key} to create and bind
            
        Returns:
            Dict with exchange and bindings information
        """
        channel = self.get_channel()
        
        # Declare the direct exchange
        exchange = exchange_manager.declare_direct_exchange(
            exchange_name=exchange_name,
            channel=channel
        )
        
        bindings = []
        
        # If direct bindings provided, create queues and bind them
        if direct_bindings:
            for queue_name, routing_key in direct_bindings.items():
                # Declare the queue
                queue_manager.declare_queue(
                    queue_name=queue_name,
                    durable=True,
                    channel=channel
                )
                
                # Bind the queue with the routing key
                binding = binding_manager.bind_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key=routing_key,
                    channel=channel
                )
                
                bindings.append({
                    'queue': queue_name,
                    'routing_key': routing_key,
                    'binding': binding
                })
                
            logger.info(f"Direct routing set up with exchange '{exchange_name}' and {len(bindings)} queues")
        else:
            logger.info(f"Direct exchange '{exchange_name}' set up (no queues bound)")
            
        return {
            'exchange': exchange,
            'bindings': bindings
        }
    
    # === Headers Routing Pattern ===
    
    def setup_headers_routing(self, exchange_name, header_bindings=None):
        """
        Set up a headers routing pattern.
        In this pattern, messages are routed based on header value matches.
        
        Args:
            exchange_name: The name of the headers exchange
            header_bindings: Optional list of dicts with keys:
                             'queue_name', 'headers', 'match_all'
            
        Returns:
            Dict with exchange and bindings information
        """
        channel = self.get_channel()
        
        # Declare the headers exchange
        exchange = exchange_manager.declare_headers_exchange(
            exchange_name=exchange_name,
            channel=channel
        )
        
        bindings = []
        
        # If header bindings provided, create queues and bind them
        if header_bindings:
            for binding_info in header_bindings:
                queue_name = binding_info['queue_name']
                headers = binding_info['headers']
                match_all = binding_info.get('match_all', True)
                
                # Declare the queue
                queue_manager.declare_queue(
                    queue_name=queue_name,
                    durable=True,
                    channel=channel
                )
                
                # Prepare arguments with x-match
                arguments = {'x-match': 'all' if match_all else 'any'}
                arguments.update(headers)
                
                # Bind the queue with header matching
                binding = binding_manager.bind_queue(
                    queue_name=queue_name,
                    exchange_name=exchange_name,
                    routing_key='',  # Routing key is ignored for headers exchanges
                    arguments=arguments,
                    channel=channel
                )
                
                bindings.append({
                    'queue': queue_name,
                    'headers': headers,
                    'match_all': match_all,
                    'binding': binding
                })
                
            logger.info(f"Headers routing set up with exchange '{exchange_name}' and {len(bindings)} queues")
        else:
            logger.info(f"Headers exchange '{exchange_name}' set up (no queues bound)")
            
        return {
            'exchange': exchange,
            'bindings': bindings
        }
    
    # === High Availability - Quorum Queues ===
    
    def setup_quorum_queue(self, queue_name, exchange_name=None, routing_key=None):
        """
        Set up a quorum queue for high availability.
        Quorum queues maintain consensus across multiple nodes for high availability.
        
        Args:
            queue_name: The name of the quorum queue
            exchange_name: Optional exchange to bind to
            routing_key: Optional routing key for binding
            
        Returns:
            Dict with queue information
        """
        channel = self.get_channel()
        
        # Declare the quorum queue
        queue = queue_manager.declare_quorum_queue(
            queue_name=queue_name,
            channel=channel
        )
        
        binding = None
        
        # If exchange provided, bind the queue
        if exchange_name:
            binding = binding_manager.bind_queue(
                queue_name=queue_name,
                exchange_name=exchange_name,
                routing_key=routing_key or queue_name,
                channel=channel
            )
        
        logger.info(f"Quorum queue '{queue_name}' set up successfully")
        
        return {
            'queue': queue,
            'binding': binding
        }
    
    # === Dead Letter Queue Configuration ===
    
    def setup_dead_letter_queue(self, queue_name, dlx_name='dead-letter', ttl=None):
        """
        Set up a queue with dead-letter configuration.
        Messages that are rejected or expire will be sent to the dead-letter queue.
        
        Args:
            queue_name: The name of the queue to create
            dlx_name: The name of the dead-letter exchange
            ttl: Optional TTL (time-to-live) for messages in milliseconds
            
        Returns:
            Dict with queue and DLQ information
        """
        channel = self.get_channel()
        
        # Declare the dead-letter exchange
        dlx = exchange_manager.declare_fanout_exchange(
            exchange_name=dlx_name,
            channel=channel
        )
        
        # Declare the dead-letter queue
        dlq_name = f"{queue_name}.dlq"
        dlq = queue_manager.declare_queue(
            queue_name=dlq_name,
            durable=True,
            channel=channel
        )
        
        # Bind the dead-letter queue to the exchange
        dlq_binding = binding_manager.bind_queue(
            queue_name=dlq_name,
            exchange_name=dlx_name,
            routing_key='',  # Fanout ignores routing key
            channel=channel
        )
        
        # Prepare arguments for the main queue
        arguments = {
            'x-dead-letter-exchange': dlx_name
        }
        
        # Add TTL if specified
        if ttl is not None:
            arguments['x-message-ttl'] = ttl
        
        # Declare the main queue with dead-letter config
        main_queue = queue_manager.declare_queue(
            queue_name=queue_name,
            durable=True,
            arguments=arguments,
            channel=channel
        )
        
        logger.info(f"Queue '{queue_name}' set up with dead-letter queue '{dlq_name}'")
        
        return {
            'queue': main_queue,
            'dlx': dlx,
            'dlq': dlq,
            'dlq_binding': dlq_binding
        }
    
    # === Request-Reply Pattern ===
    
    def create_request_reply_pattern(
        self,
        request_exchange: str,
        request_key: str,
        reply_queue: str = None,
        ttl: int = 30000,  # 30 seconds
        durable: bool = False
    ):
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
        channel = self.get_channel()
        
        # Declare the request exchange if it doesn't exist
        exchange = exchange_manager.declare_direct_exchange(
            exchange_name=request_exchange,
            channel=channel
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
            queue_manager.declare_queue(
                queue_name=reply_queue,
                durable=durable,
                arguments={'x-message-ttl': ttl},
                channel=channel
            )
        
        logger.info(f"Created request-reply pattern with reply queue {reply_queue}")
        
        return {
            'request_exchange': exchange,
            'request_key': request_key,
            'reply_queue': reply_queue
        }
    
    # === Message Publishing ===
    
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
    ):
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
            channel = self.get_channel()
            
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
    
    # === Consumer Registration ===
    
    def register_consumer(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = False,
        prefetch_count: int = 1,
        consumer_name: str = '',
        arguments: Dict[str, Any] = None
    ):
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
            channel = self.get_channel()
            
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
    
    # === Message Failure Handling ===
    
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
    
    # === Cleanup ===
    
    def cleanup(self):
        """Clean up resources."""
        if self._channel and self._channel.is_open:
            self._channel.close()
            self._channel = None

# Singleton instance
message_router = MessageRouter()