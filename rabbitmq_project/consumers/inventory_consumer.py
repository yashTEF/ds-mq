"""
Inventory consumer module for processing order events related to inventory.
Extends the base consumer with inventory-specific functionality.
"""
import json
import time
import random
from .base_consumer import BaseConsumer
from ..core.logger import get_logger

# Module logger
logger = get_logger(__name__)

class InventoryConsumer(BaseConsumer):
    """
    Consumer for inventory-related order events.
    Processes order messages and updates inventory accordingly.
    """
    
    def __init__(self, queue_name='inventory', exchange_name='orders',
                exchange_type='topic', routing_key='order.created.*',
                auto_ack=False, prefetch_count=10):
        """
        Initialize the inventory consumer.
        
        Args:
            queue_name: Name of the queue to consume from
            exchange_name: Name of the exchange to bind to
            exchange_type: Type of exchange (default: topic for order events)
            routing_key: Routing key pattern to listen for
            auto_ack: Whether to auto-acknowledge messages
            prefetch_count: Number of messages to prefetch
        """
        super().__init__(
            queue_name=queue_name,
            exchange_name=exchange_name,
            exchange_type=exchange_type,
            routing_key=routing_key,
            auto_ack=auto_ack,
            prefetch_count=prefetch_count,
            declare_queue=True,
            durable_queue=True
        )
        
        # Simulate inventory database
        self.inventory = {}
        self.initialize_inventory()
        
    def initialize_inventory(self):
        """Initialize the simulated inventory with some products."""
        self.inventory = {
            'product1': {'name': 'Product 1', 'quantity': 100},
            'product2': {'name': 'Product 2', 'quantity': 50},
            'product3': {'name': 'Product 3', 'quantity': 75},
            'product4': {'name': 'Product 4', 'quantity': 30},
            'product5': {'name': 'Product 5', 'quantity': 0}  # Out of stock
        }
        logger.info(f"Initialized inventory with {len(self.inventory)} products")
        
    def get_inventory_status(self, product_id):
        """
        Get the status of a product in the inventory.
        
        Args:
            product_id: ID of the product to check
            
        Returns:
            Tuple of (product, quantity, in_stock)
        """
        if product_id not in self.inventory:
            return None, 0, False
            
        product = self.inventory[product_id]
        quantity = product['quantity']
        in_stock = quantity > 0
        
        return product, quantity, in_stock
        
    def update_inventory(self, product_id, quantity_change):
        """
        Update the inventory for a product.
        
        Args:
            product_id: ID of the product to update
            quantity_change: Change in quantity (negative for decrements)
            
        Returns:
            True if update successful, False otherwise
        """
        if product_id not in self.inventory:
            logger.warning(f"Cannot update inventory: Product {product_id} not found")
            return False
            
        current_quantity = self.inventory[product_id]['quantity']
        new_quantity = current_quantity + quantity_change
        
        if new_quantity < 0:
            logger.warning(f"Insufficient inventory for {product_id}: " 
                          f"requested {-quantity_change}, available {current_quantity}")
            return False
            
        self.inventory[product_id]['quantity'] = new_quantity
        logger.info(f"Updated inventory for {product_id}: {current_quantity} -> {new_quantity}")
        return True
        
    def process_order_created(self, order_data):
        """
        Process an order created event.
        
        Args:
            order_data: The order data from the message
            
        Returns:
            True if processing successful, False otherwise
        """
        try:
            # Extract order details
            order_id = order_data.get('id', 'unknown')
            customer_id = order_data.get('customer_id', 'unknown')
            items = order_data.get('items', [])
            
            logger.info(f"Processing order {order_id} for customer {customer_id} with {len(items)} items")
            
            # Simulate processing time
            time.sleep(0.5)
            
            # Process each item in the order
            all_items_available = True
            processed_items = []
            
            for item in items:
                product_id = item.get('product_id')
                quantity = item.get('quantity', 1)
                
                # Check item availability
                product, available_quantity, in_stock = self.get_inventory_status(product_id)
                
                if not in_stock or available_quantity < quantity:
                    logger.warning(f"Item {product_id} not available in requested quantity")
                    all_items_available = False
                    processed_items.append({
                        'product_id': product_id,
                        'requested': quantity,
                        'available': available_quantity,
                        'status': 'unavailable'
                    })
                    continue
                    
                # Try to update inventory (decrement quantity)
                if self.update_inventory(product_id, -quantity):
                    processed_items.append({
                        'product_id': product_id,
                        'quantity': quantity,
                        'status': 'reserved'
                    })
                else:
                    all_items_available = False
                    processed_items.append({
                        'product_id': product_id,
                        'quantity': quantity,
                        'status': 'failed'
                    })
            
            # Create inventory response
            inventory_result = {
                'order_id': order_id,
                'customer_id': customer_id,
                'status': 'fulfilled' if all_items_available else 'partial',
                'processed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'items': processed_items
            }
            
            logger.info(f"Order {order_id} processed with status: {inventory_result['status']}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing order: {e}")
            # Return False to trigger retry
            return False
            
    def handle_message(self, channel, method, properties, body):
        """
        Handle a received message.
        
        Args:
            channel: The channel from which the message was received
            method: The pika.spec.Basic.Deliver method
            properties: The pika.spec.BasicProperties properties
            body: The message body (parsed if JSON)
            
        Returns:
            True if message was handled successfully, False otherwise
        """
        try:
            logger.info(f"Received message with routing key: {method.routing_key}")
            
            # Randomly simulate errors (20% chance)
            if random.random() < 0.2:
                logger.warning("Simulating random processing error")
                return False
                
            # Handle based on routing key
            if method.routing_key.startswith('order.created'):
                if isinstance(body, dict) and 'data' in body:
                    return self.process_order_created(body['data'])
                else:
                    logger.warning(f"Invalid order created message format: {body}")
                    return False
                    
            elif method.routing_key == 'order.updated':
                logger.info(f"Processing order updated event: {body}")
                # Handle order updates if needed
                return True
                
            else:
                logger.warning(f"Unhandled routing key: {method.routing_key}")
                # We still acknowledge the message as there's no point retrying
                # a message we don't know how to handle
                return True
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return False