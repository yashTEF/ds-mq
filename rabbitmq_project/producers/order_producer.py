"""
Order producer module for publishing order events to RabbitMQ.
Extends the base producer with order-specific functionality.
"""
from .base_producer import BaseProducer
from ..core.logger import get_logger

# Module logger
logger = get_logger(__name__)

class OrderProducer(BaseProducer):
    """
    Producer for order-related events.
    Formats order payloads and publishes to appropriate exchanges with relevant routing keys.
    """
    
    def __init__(self, exchange_name='orders', exchange_type='topic', 
                routing_key='order', use_publisher_confirms=True):
        """
        Initialize the order producer.
        
        Args:
            exchange_name: Name of the exchange for order events
            exchange_type: Type of exchange (default: topic for order events)
            routing_key: Default routing key prefix
            use_publisher_confirms: Whether to use publisher confirms
        """
        super().__init__(
            exchange_name=exchange_name,
            exchange_type=exchange_type,
            routing_key=routing_key,
            declare_exchange=True,
            use_publisher_confirms=use_publisher_confirms
        )
        
    def publish_order_created(self, order_data):
        """
        Publish an order created event.
        
        Args:
            order_data: Dictionary containing order details
            
        Returns:
            True if published successfully
        """
        # Ensure order has required fields
        if 'id' not in order_data:
            order_data['id'] = f"order-{order_data.get('customer_id', '')}-{order_data.get('timestamp', '')}"
            
        logger.info(f"Publishing order created event: {order_data.get('id')}")
        
        # Add event type to payload
        payload = {
            'event_type': 'created',
            'data': order_data
        }
        
        # Use routing key pattern: order.created.{customer_type}
        customer_type = order_data.get('customer_type', 'standard')
        routing_key = f"order.created.{customer_type}"
        
        return self.publish(
            message=payload,
            routing_key=routing_key,
            headers={'event': 'created', 'priority': order_data.get('priority', 'normal')}
        )
        
    def publish_order_updated(self, order_id, updates):
        """
        Publish an order updated event.
        
        Args:
            order_id: ID of the order being updated
            updates: Dictionary of fields being updated
            
        Returns:
            True if published successfully
        """
        logger.info(f"Publishing order updated event: {order_id}")
        
        # Create payload
        payload = {
            'event_type': 'updated',
            'order_id': order_id,
            'updates': updates
        }
        
        # Use routing key pattern: order.updated
        routing_key = "order.updated"
        
        return self.publish(
            message=payload,
            routing_key=routing_key,
            headers={'event': 'updated', 'order_id': order_id}
        )
        
    def publish_order_cancelled(self, order_id, reason=None):
        """
        Publish an order cancelled event.
        
        Args:
            order_id: ID of the order being cancelled
            reason: Optional reason for cancellation
            
        Returns:
            True if published successfully
        """
        logger.info(f"Publishing order cancelled event: {order_id}")
        
        # Create payload
        payload = {
            'event_type': 'cancelled',
            'order_id': order_id,
            'reason': reason
        }
        
        # Use routing key pattern: order.cancelled
        routing_key = "order.cancelled"
        
        return self.publish(
            message=payload,
            routing_key=routing_key,
            headers={'event': 'cancelled', 'order_id': order_id}
        )
        
    def publish_order_fulfilled(self, order_id, fulfillment_details):
        """
        Publish an order fulfilled event.
        
        Args:
            order_id: ID of the order being fulfilled
            fulfillment_details: Dictionary with fulfillment information
            
        Returns:
            True if published successfully
        """
        logger.info(f"Publishing order fulfilled event: {order_id}")
        
        # Create payload
        payload = {
            'event_type': 'fulfilled',
            'order_id': order_id,
            'fulfillment': fulfillment_details
        }
        
        # Use routing key pattern: order.fulfilled
        routing_key = "order.fulfilled"
        
        return self.publish(
            message=payload,
            routing_key=routing_key,
            headers={'event': 'fulfilled', 'order_id': order_id}
        )
        
    def publish_order_to_direct_exchange(self, order_data, routing_key):
        """
        Publish an order to a direct exchange with a specific routing key.
        Useful for directing orders to specific processing queues.
        
        Args:
            order_data: Dictionary containing order details
            routing_key: Specific routing key for the order
            
        Returns:
            True if published successfully
        """
        direct_exchange = f"{self.exchange_name}.direct"
        
        # Ensure exchange exists
        channel = self.get_channel()
        channel.exchange_declare(
            exchange=direct_exchange,
            exchange_type='direct',
            durable=True
        )
        
        return self.publish(
            message=order_data,
            routing_key=routing_key,
            exchange_name=direct_exchange
        )
        
    def broadcast_order_notification(self, notification):
        """
        Broadcast an order notification to all subscribers via fanout exchange.
        
        Args:
            notification: Dictionary containing notification details
            
        Returns:
            True if published successfully
        """
        fanout_exchange = f"{self.exchange_name}.notifications"
        
        # Ensure exchange exists
        channel = self.get_channel()
        channel.exchange_declare(
            exchange=fanout_exchange,
            exchange_type='fanout',
            durable=True
        )
        
        return self.publish(
            message=notification,
            routing_key='',  # Ignored for fanout
            exchange_name=fanout_exchange
        )