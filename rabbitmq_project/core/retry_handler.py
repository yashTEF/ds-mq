"""
Retry handler for managing failed message processing attempts.
Implements exponential backoff and dead-letter queue mechanisms.
"""
import time
import json
from datetime import datetime
import pika
from ..config.config import config
from .logger import get_logger
from .channel_manager import get_channel

# Module logger
logger = get_logger(__name__)

class RetryHandler:
    """
    Handles retry logic for failed message processing.
    Supports exponential backoff and dead-letter queues.
    """
    
    def __init__(self, retry_exchange="retry", 
                 dead_letter_exchange="dead-letter",
                 max_retries=None):
        """
        Initialize retry handler.
        
        Args:
            retry_exchange: Name of the exchange for retry messages
            dead_letter_exchange: Name of the exchange for dead-lettered messages
            max_retries: Maximum number of retries before dead-lettering
        """
        self.retry_exchange = retry_exchange
        self.dead_letter_exchange = dead_letter_exchange
        self.max_retries = max_retries or config.CONSUMER_MAX_RETRIES
        self.backoff_factor = config.CONSUMER_RETRY_BACKOFF_FACTOR
        self.base_delay = config.CONSUMER_RETRY_DELAY
        
    def setup(self, channel=None):
        """
        Set up required exchanges and queues for retry mechanism.
        
        Args:
            channel: Optional channel to use, otherwise gets one
        """
        if channel is None:
            channel = get_channel()
            
        # Declare retry exchange (direct type)
        channel.exchange_declare(
            exchange=self.retry_exchange,
            exchange_type='direct',
            durable=True
        )
        
        # Declare dead-letter exchange (fanout type)
        channel.exchange_declare(
            exchange=self.dead_letter_exchange,
            exchange_type='fanout',
            durable=True
        )
        
        # Declare dead-letter queue
        channel.queue_declare(
            queue=f"{self.dead_letter_exchange}.queue",
            durable=True
        )
        
        # Bind dead-letter queue to exchange
        channel.queue_bind(
            queue=f"{self.dead_letter_exchange}.queue",
            exchange=self.dead_letter_exchange,
            routing_key='#'
        )
        
        logger.info("Retry and dead-letter exchanges/queues set up")
    
    def process_retry(self, channel, method, properties, body, exception=None, queue_name=None):
        """
        Process a message that has failed and needs to be retried.
        
        Args:
            channel: The channel from which the message was consumed
            method: The method from the consume callback
            properties: The message properties
            body: The message body
            exception: The exception that caused the failure
            queue_name: The original queue name
            
        Returns:
            bool: True if message was requeued for retry, False if dead-lettered
        """
        try:
            # Get or initialize retry count
            headers = properties.headers or {}
            retry_count = headers.get('x-retry-count', 0) + 1
            original_queue = queue_name or headers.get('x-original-queue', method.routing_key)
            
            if retry_count <= self.max_retries:
                # Calculate delay with exponential backoff
                delay = self.base_delay * (self.backoff_factor ** (retry_count - 1))
                
                logger.info(f"Retry {retry_count}/{self.max_retries} for message from "
                           f"queue '{original_queue}' with delay of {delay}s")
                
                # Update headers for retry
                headers.update({
                    'x-retry-count': retry_count,
                    'x-original-queue': original_queue,
                    'x-exception': str(exception) if exception else 'Unknown error',
                    'x-retry-time': datetime.utcnow().isoformat()
                })
                
                # Create new properties with updated headers
                retry_properties = pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers=headers,
                    expiration=str(int(delay * 1000))  # Expiration in milliseconds
                )
                
                # Create retry queue with TTL and dead-letter config if it doesn't exist
                retry_queue = f"{self.retry_exchange}.{original_queue}.{delay}"
                channel.queue_declare(
                    queue=retry_queue,
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': '',  # Default exchange
                        'x-dead-letter-routing-key': original_queue,
                        'x-message-ttl': int(delay * 1000)
                    }
                )
                
                # Bind retry queue to retry exchange
                channel.queue_bind(
                    queue=retry_queue,
                    exchange=self.retry_exchange,
                    routing_key=retry_queue
                )
                
                # Publish message to retry queue
                channel.basic_publish(
                    exchange=self.retry_exchange,
                    routing_key=retry_queue,
                    body=body,
                    properties=retry_properties
                )
                
                # Acknowledge the original message
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return True
                
            else:
                # Max retries exceeded, send to dead-letter queue
                logger.warning(f"Message exceeded max retries ({self.max_retries}), "
                              f"moving to dead-letter queue: {body}")
                
                # Update headers for dead-letter
                headers.update({
                    'x-retry-count': retry_count,
                    'x-original-queue': original_queue,
                    'x-exception': str(exception) if exception else 'Max retries exceeded',
                    'x-dead-letter-time': datetime.utcnow().isoformat()
                })
                
                # Create new properties with updated headers
                dead_letter_properties = pika.BasicProperties(
                    delivery_mode=2,
                    headers=headers
                )
                
                # Publish to dead-letter exchange
                channel.basic_publish(
                    exchange=self.dead_letter_exchange,
                    routing_key='',
                    body=body,
                    properties=dead_letter_properties
                )
                
                # Acknowledge the original message
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return False
                
        except Exception as e:
            logger.error(f"Error during retry handling: {e}")
            # Reject message, requeue it as fallback
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            raise

# Singleton instance
retry_handler = RetryHandler()