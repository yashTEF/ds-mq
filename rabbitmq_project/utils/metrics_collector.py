"""
Metrics collector utility for tracking message metrics.
Provides prometheus metrics for message rates, processing times, and errors.
"""
import time
from prometheus_client import Counter, Gauge, Histogram, Summary
from ..core.logger import get_logger

# Module logger
logger = get_logger(__name__)

class MetricsCollector:
    """
    Collects and exposes metrics related to message processing.
    Uses prometheus_client primitives to track various metrics.
    """
    
    def __init__(self):
        """Initialize metrics collector with Prometheus metrics."""
        # Message counts by exchange, queue, and routing key
        self.messages_published = Counter(
            'rabbitmq_client_messages_published_total',
            'Total number of messages published',
            ['exchange', 'routing_key']
        )
        
        self.messages_consumed = Counter(
            'rabbitmq_client_messages_consumed_total',
            'Total number of messages consumed',
            ['queue', 'consumer_id']
        )
        
        self.messages_acknowledged = Counter(
            'rabbitmq_client_messages_acknowledged_total',
            'Total number of messages acknowledged',
            ['queue', 'consumer_id']
        )
        
        self.messages_rejected = Counter(
            'rabbitmq_client_messages_rejected_total',
            'Total number of messages rejected',
            ['queue', 'consumer_id']
        )
        
        self.retry_count = Counter(
            'rabbitmq_client_retry_total',
            'Total number of message retries',
            ['queue']
        )
        
        self.dead_letter_count = Counter(
            'rabbitmq_client_dead_letter_total',
            'Total number of messages sent to dead-letter queue',
            ['source_queue', 'dead_letter_queue']
        )
        
        # Processing durations
        self.publish_duration = Histogram(
            'rabbitmq_client_publish_duration_seconds',
            'Time taken to publish a message',
            ['exchange'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        )
        
        self.consume_duration = Histogram(
            'rabbitmq_client_consume_duration_seconds',
            'Time taken to process a consumed message',
            ['queue', 'consumer_id'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        )
        
        # Current message counts
        self.queue_message_count = Gauge(
            'rabbitmq_client_queue_messages',
            'Current number of messages in a queue',
            ['queue']
        )
        
        self.consumer_count = Gauge(
            'rabbitmq_client_queue_consumers',
            'Current number of consumers for a queue',
            ['queue']
        )
        
        # Error metrics
        self.publish_errors = Counter(
            'rabbitmq_client_publish_errors_total',
            'Total number of publish errors',
            ['exchange', 'error_type']
        )
        
        self.consume_errors = Counter(
            'rabbitmq_client_consume_errors_total',
            'Total number of consume errors',
            ['queue', 'error_type']
        )
        
    def observe_publish(self, exchange, routing_key='', error_type=None):
        """
        Create a context manager to track message publishing metrics.
        
        Args:
            exchange: The exchange the message is being published to
            routing_key: The routing key used (default: '')
            error_type: Optional error type if an error occurred
            
        Returns:
            A context manager that tracks publish duration and increments counters
        """
        return _PublishMetricContext(self, exchange, routing_key, error_type)
        
    def observe_consume(self, queue, consumer_id='default', error_type=None):
        """
        Create a context manager to track message consumption metrics.
        
        Args:
            queue: The queue the message is being consumed from
            consumer_id: ID of the consumer (default: 'default')
            error_type: Optional error type if an error occurred
            
        Returns:
            A context manager that tracks consume duration and increments counters
        """
        return _ConsumeMetricContext(self, queue, consumer_id, error_type)
        
    def track_publish(self, exchange, routing_key=''):
        """
        Track a published message.
        
        Args:
            exchange: The exchange the message was published to
            routing_key: The routing key used
        """
        self.messages_published.labels(
            exchange=exchange,
            routing_key=routing_key
        ).inc()
        
    def track_consume(self, queue, consumer_id='default'):
        """
        Track a consumed message.
        
        Args:
            queue: The queue the message was consumed from
            consumer_id: ID of the consumer
        """
        self.messages_consumed.labels(
            queue=queue,
            consumer_id=consumer_id
        ).inc()
        
    def track_ack(self, queue, consumer_id='default'):
        """
        Track an acknowledged message.
        
        Args:
            queue: The queue the message was consumed from
            consumer_id: ID of the consumer
        """
        self.messages_acknowledged.labels(
            queue=queue,
            consumer_id=consumer_id
        ).inc()
        
    def track_reject(self, queue, consumer_id='default'):
        """
        Track a rejected message.
        
        Args:
            queue: The queue the message was consumed from
            consumer_id: ID of the consumer
        """
        self.messages_rejected.labels(
            queue=queue,
            consumer_id=consumer_id
        ).inc()
        
    def track_retry(self, queue):
        """
        Track a message retry.
        
        Args:
            queue: The queue the message was consumed from
        """
        self.retry_count.labels(queue=queue).inc()
        
    def track_dead_letter(self, source_queue, dead_letter_queue):
        """
        Track a message sent to a dead-letter queue.
        
        Args:
            source_queue: The original queue the message was in
            dead_letter_queue: The dead-letter queue the message was sent to
        """
        self.dead_letter_count.labels(
            source_queue=source_queue,
            dead_letter_queue=dead_letter_queue
        ).inc()
        
    def track_publish_error(self, exchange, error_type):
        """
        Track a publish error.
        
        Args:
            exchange: The exchange the message was being published to
            error_type: The type of error
        """
        self.publish_errors.labels(
            exchange=exchange,
            error_type=error_type
        ).inc()
        
    def track_consume_error(self, queue, error_type):
        """
        Track a consume error.
        
        Args:
            queue: The queue the message was being consumed from
            error_type: The type of error
        """
        self.consume_errors.labels(
            queue=queue,
            error_type=error_type
        ).inc()
        
    def set_queue_message_count(self, queue, count):
        """
        Set the current message count for a queue.
        
        Args:
            queue: The queue name
            count: The current message count
        """
        self.queue_message_count.labels(queue=queue).set(count)
        
    def set_consumer_count(self, queue, count):
        """
        Set the current consumer count for a queue.
        
        Args:
            queue: The queue name
            count: The current consumer count
        """
        self.consumer_count.labels(queue=queue).set(count)

class _PublishMetricContext:
    """Context manager for tracking publish metrics."""
    
    def __init__(self, collector, exchange, routing_key='', error_type=None):
        """
        Initialize publish metric context.
        
        Args:
            collector: The metrics collector
            exchange: The exchange the message is being published to
            routing_key: The routing key used
            error_type: Optional error type if an error occurred
        """
        self.collector = collector
        self.exchange = exchange
        self.routing_key = routing_key
        self.error_type = error_type
        self.start_time = None
        
    def __enter__(self):
        """Start timing when entering the context."""
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Record metrics when exiting the context."""
        # Record duration
        duration = time.time() - self.start_time
        self.collector.publish_duration.labels(
            exchange=self.exchange
        ).observe(duration)
        
        if exc_type is None:
            # Success
            self.collector.track_publish(
                exchange=self.exchange,
                routing_key=self.routing_key
            )
        else:
            # Error
            error_name = self.error_type or exc_type.__name__
            self.collector.track_publish_error(
                exchange=self.exchange,
                error_type=error_name
            )
        
        # Don't suppress exceptions
        return False

class _ConsumeMetricContext:
    """Context manager for tracking consume metrics."""
    
    def __init__(self, collector, queue, consumer_id='default', error_type=None):
        """
        Initialize consume metric context.
        
        Args:
            collector: The metrics collector
            queue: The queue the message is being consumed from
            consumer_id: ID of the consumer
            error_type: Optional error type if an error occurred
        """
        self.collector = collector
        self.queue = queue
        self.consumer_id = consumer_id
        self.error_type = error_type
        self.start_time = None
        
    def __enter__(self):
        """Start timing when entering the context."""
        self.start_time = time.time()
        self.collector.track_consume(
            queue=self.queue,
            consumer_id=self.consumer_id
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Record metrics when exiting the context."""
        # Record duration
        duration = time.time() - self.start_time
        self.collector.consume_duration.labels(
            queue=self.queue,
            consumer_id=self.consumer_id
        ).observe(duration)
        
        if exc_type is None:
            # Success
            self.collector.track_ack(
                queue=self.queue,
                consumer_id=self.consumer_id
            )
        else:
            # Error
            error_name = self.error_type or exc_type.__name__
            self.collector.track_consume_error(
                queue=self.queue,
                error_type=error_name
            )
            self.collector.track_reject(
                queue=self.queue,
                consumer_id=self.consumer_id
            )
        
        # Don't suppress exceptions
        return False

# Singleton instance
metrics_collector = MetricsCollector()