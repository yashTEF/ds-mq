"""
Base consumer module for consuming messages from RabbitMQ.
Provides common consumption functionality with acknowledgment and error handling.
"""
import json
import threading
import time
from functools import wraps
import pika
from ..core.channel_manager import get_channel
from ..core.logger import get_logger
from ..core.retry_handler import retry_handler

# Module logger
logger = get_logger(__name__)

def handle_channel_exception(func):
    """Decorator to handle channel exceptions in consumer methods."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except pika.exceptions.AMQPChannelError as e:
            logger.error(f"AMQP Channel Error: {e}")
            self._close_channel()
            raise
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"AMQP Connection Error: {e}")
            self._close_channel()
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self._close_channel()
            raise
    return wrapper

class BaseConsumer:
    """
    Base class for RabbitMQ consumers.
    Handles common consumption logic with support for queue declaration,
    acknowledgments, and error handling.
    """
    
    def __init__(self, queue_name, prefetch_count=1, auto_ack=False,
                exchange_name=None, exchange_type=None,
                routing_key=None, declare_queue=True,
                durable_queue=True, exclusive_queue=False,
                auto_delete_queue=False):
        """
        Initialize the consumer.
        
        Args:
            queue_name: Name of the queue to consume from
            prefetch_count: Number of messages to prefetch (1 for fair dispatch)
            auto_ack: Whether to auto-acknowledge messages
            exchange_name: Name of exchange to bind to (if any)
            exchange_type: Type of exchange ('direct', 'topic', 'fanout', 'headers')
            routing_key: Routing key for binding the queue to the exchange
            declare_queue: Whether to declare the queue if it doesn't exist
            durable_queue: Whether the queue should survive broker restarts
            exclusive_queue: Whether the queue should be used by only one connection
            auto_delete_queue: Whether the queue should be deleted when no longer used
        """
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.auto_ack = auto_ack
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_key = routing_key or queue_name
        self.declare_queue = declare_queue
        self.durable_queue = durable_queue
        self.exclusive_queue = exclusive_queue
        self.auto_delete_queue = auto_delete_queue
        
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._consuming_thread = None
        self._stop_event = threading.Event()
        
    def _setup_channel(self):
        """
        Setup the channel for consuming.
        Declares the queue if required and sets QoS.
        
        Returns:
            A pika.channel.Channel ready for consuming
        """
        channel = get_channel()
        
        # Set QoS
        channel.basic_qos(prefetch_count=self.prefetch_count)
        
        # Declare the queue if requested
        if self.declare_queue:
            channel.queue_declare(
                queue=self.queue_name,
                durable=self.durable_queue,
                exclusive=self.exclusive_queue,
                auto_delete=self.auto_delete_queue
            )
            
        # If exchange is specified, declare it and bind the queue
        if self.exchange_name and self.exchange_type:
            channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=self.exchange_type,
                durable=True
            )
            
            channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.routing_key
            )
            
        return channel
        
    def get_channel(self):
        """
        Get a channel for consuming, creating a new one if needed.
        
        Returns:
            A pika.channel.Channel
        """
        if self._channel is None or not self._channel.is_open:
            self._channel = self._setup_channel()
            
        return self._channel
        
    def _close_channel(self):
        """Close the channel if it's open."""
        if self._channel and self._channel.is_open:
            try:
                self._channel.close()
                logger.debug("Consumer channel closed")
            except Exception as e:
                logger.warning(f"Error closing consumer channel: {e}")
            finally:
                self._channel = None
                
    @handle_channel_exception
    def _process_message(self, channel, method, properties, body):
        """
        Process a received message.
        This is a wrapper that calls the user-defined callback method.
        
        Args:
            channel: The channel from which the message was received
            method: The pika.spec.Basic.Deliver method
            properties: The pika.spec.BasicProperties properties
            body: The message body
        """
        try:
            # Try to parse JSON content
            content = None
            if properties.content_type == 'application/json':
                try:
                    content = json.loads(body.decode('utf-8'))
                except json.JSONDecodeError:
                    content = body.decode('utf-8')
            else:
                content = body.decode('utf-8')
                
            # Call the handler method
            result = self.handle_message(channel, method, properties, content)
            
            # Handle ACK based on result and auto_ack setting
            if not self.auto_ack:
                if result:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    logger.debug(f"Message acknowledged: {method.delivery_tag}")
                else:
                    # Handle retry with our retry_handler
                    retry_handler.process_retry(
                        channel, method, properties, body, 
                        queue_name=self.queue_name
                    )
                    logger.debug(f"Message sent to retry handler: {method.delivery_tag}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            if not self.auto_ack:
                retry_handler.process_retry(
                    channel, method, properties, body, 
                    exception=e,
                    queue_name=self.queue_name
                )
        
    def handle_message(self, channel, method, properties, body):
        """
        Handle a received message.
        This method should be overridden by subclasses.
        
        Args:
            channel: The channel from which the message was received
            method: The pika.spec.Basic.Deliver method
            properties: The pika.spec.BasicProperties properties
            body: The message body (parsed if JSON)
            
        Returns:
            True if message was handled successfully, False otherwise
        """
        logger.info(f"Received message: {body}")
        return True
        
    @handle_channel_exception
    def start_consuming(self, block=True):
        """
        Start consuming messages from the queue.
        
        Args:
            block: Whether to block the thread while consuming
        """
        if self._consuming:
            logger.warning("Already consuming, ignoring start_consuming request")
            return
            
        channel = self.get_channel()
        
        # Set up the consumer
        self._consumer_tag = channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._process_message,
            auto_ack=self.auto_ack
        )
        
        self._consuming = True
        self._stop_event.clear()
        
        logger.info(f"Started consuming from queue '{self.queue_name}'")
        
        if block:
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                self.stop_consuming()
        else:
            self._consuming_thread = threading.Thread(
                target=self._consume_messages
            )
            self._consuming_thread.daemon = True
            self._consuming_thread.start()
            
    def _consume_messages(self):
        """Target for the consumer thread when running in non-blocking mode."""
        try:
            channel = self.get_channel()
            while not self._stop_event.is_set():
                try:
                    channel.connection.process_data_events(time_limit=0.1)
                except Exception as e:
                    logger.error(f"Error processing events: {e}")
                    if not self._closing:
                        time.sleep(0.5)
                        # Try to reestablish the channel
                        try:
                            self._channel = self._setup_channel()
                            # Set up the consumer again
                            self._consumer_tag = self._channel.basic_consume(
                                queue=self.queue_name,
                                on_message_callback=self._process_message,
                                auto_ack=self.auto_ack
                            )
                        except Exception as setup_error:
                            logger.error(f"Failed to reestablish connection: {setup_error}")
                            time.sleep(5)  # Delay before retrying
        finally:
            self._consuming = False
            
    def stop_consuming(self, wait=True):
        """
        Stop consuming messages.
        
        Args:
            wait: Whether to wait for the consumer thread to finish
        """
        if not self._consuming:
            logger.warning("Not consuming, ignoring stop_consuming request")
            return
            
        self._closing = True
        self._stop_event.set()
        
        try:
            if self._channel and self._channel.is_open and self._consumer_tag:
                self._channel.basic_cancel(self._consumer_tag)
                
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except Exception as e:
            logger.warning(f"Error stopping consumer: {e}")
            
        self._consumer_tag = None
        self._consuming = False
        
        # Wait for consumer thread if running in non-blocking mode
        if wait and self._consuming_thread and self._consuming_thread.is_alive():
            self._consuming_thread.join(timeout=5.0)
            
        self._close_channel()
        self._closing = False
        
        logger.info(f"Stopped consuming from queue '{self.queue_name}'")
        
    def close(self):
        """Close the consumer and its channel."""
        self.stop_consuming()
        
    def __enter__(self):
        """Enter context manager."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager."""
        self.close()