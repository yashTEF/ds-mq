"""
Base producer module for publishing messages to RabbitMQ.
Provides common publishing functionality with persistence and optional confirmations.
"""
import json
import uuid
import time
import pika
from ..core.channel_manager import get_channel
from ..core.logger import get_logger
from ..config.config import config

# Module logger
logger = get_logger(__name__)

class BaseProducer:
    """
    Base class for RabbitMQ producers.
    Handles common publishing logic with support for persistent messages and publisher confirms.
    """
    
    def __init__(self, exchange_name=None, exchange_type=None, 
                routing_key=None, declare_exchange=True,
                use_publisher_confirms=False):
        """
        Initialize the producer.
        
        Args:
            exchange_name: Name of the exchange to publish to (None for default exchange)
            exchange_type: Type of exchange ('direct', 'topic', 'fanout', 'headers')
            routing_key: Default routing key for messages
            declare_exchange: Whether to declare the exchange if it doesn't exist
            use_publisher_confirms: Whether to use publisher confirms for reliability
        """
        self.exchange_name = exchange_name or ''
        self.exchange_type = exchange_type or 'direct'
        self.routing_key = routing_key or ''
        self.declare_exchange = declare_exchange
        self.use_publisher_confirms = use_publisher_confirms
        self._channel = None
        
    def _setup_channel(self):
        """
        Setup the channel for publishing.
        Declares the exchange if required and enables publisher confirms if enabled.
        
        Returns:
            A pika.channel.Channel ready for publishing
        """
        channel = get_channel()
        
        # Enable publisher confirms if requested
        if self.use_publisher_confirms:
            channel.confirm_delivery()
            
        # Declare the exchange if it doesn't exist and we're not using default exchange
        if self.declare_exchange and self.exchange_name:
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=config.DEFAULT_EXCHANGE_DURABLE
            )
            
        return channel
        
    def get_channel(self):
        """
        Get a channel for publishing, creating a new one if needed.
        
        Returns:
            A pika.channel.Channel
        """
        if self._channel is None or not self._channel.is_open:
            self._channel = self._setup_channel()
            
        return self._channel
        
    def publish(self, message, routing_key=None, exchange_name=None, 
               properties=None, mandatory=False, headers=None,
               content_type='application/json', retry_count=3):
        """
        Publish a message to RabbitMQ.
        
        Args:
            message: Message to publish (string or dict, will be JSON serialized if dict)
            routing_key: Routing key for the message (defaults to producer's routing_key)
            exchange_name: Exchange to publish to (defaults to producer's exchange_name)
            properties: Optional message properties
            mandatory: Whether the message must be routable
            headers: Optional headers for the message
            content_type: Content type of the message
            retry_count: Number of retries on failure
            
        Returns:
            True if published successfully, False otherwise
        """
        exchange = exchange_name or self.exchange_name
        route = routing_key or self.routing_key
        
        # Convert dict to JSON string if needed
        if isinstance(message, dict):
            message = json.dumps(message)
        elif not isinstance(message, str):
            message = str(message)
            
        # Serialize message as bytes
        message_bytes = message.encode('utf-8')
        
        # Set default persistent properties if not provided
        if properties is None:
            props_kwargs = {
                'delivery_mode': 2,  # Make message persistent
                'content_type': content_type,
                'message_id': str(uuid.uuid4()),
                'timestamp': int(time.time()),
            }
            
            if headers:
                props_kwargs['headers'] = headers
                
            properties = pika.BasicProperties(**props_kwargs)
            
        # Get a channel
        channel = self.get_channel()
        
        # Attempt to publish with retries
        for attempt in range(retry_count):
            try:
                if self.use_publisher_confirms:
                    # With publisher confirms
                    confirmation = channel.basic_publish(
                        exchange=exchange,
                        routing_key=route,
                        body=message_bytes,
                        properties=properties,
                        mandatory=mandatory
                    )
                    
                    if confirmation:
                        logger.debug(f"Message confirmed: {message[:100]}...")
                        return True
                    else:
                        logger.warning(f"Message not confirmed, attempt {attempt + 1}/{retry_count}")
                        if attempt == retry_count - 1:
                            logger.error(f"Failed to publish message after {retry_count} attempts")
                            return False
                else:
                    # Without publisher confirms
                    channel.basic_publish(
                        exchange=exchange,
                        routing_key=route,
                        body=message_bytes,
                        properties=properties,
                        mandatory=mandatory
                    )
                    logger.debug(f"Message published: {message[:100]}...")
                    return True
                    
            except pika.exceptions.UnroutableError:
                logger.error(f"Message unroutable: exchange={exchange}, routing_key={route}")
                return False
                
            except Exception as e:
                logger.warning(f"Publish failed, attempt {attempt + 1}/{retry_count}: {e}")
                if attempt < retry_count - 1:
                    # Try to refresh the channel
                    try:
                        self._channel = self._setup_channel()
                    except Exception as channel_ex:
                        logger.error(f"Failed to refresh channel: {channel_ex}")
                else:
                    logger.error(f"Failed to publish message after {retry_count} attempts")
                    return False
                    
        return False
        
    def close(self):
        """Close the channel if it's open."""
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
                logger.debug("Producer channel closed")
            except Exception as e:
                logger.warning(f"Error closing producer channel: {e}")
            finally:
                self._channel = None