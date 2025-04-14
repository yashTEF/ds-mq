#!/usr/bin/env python
"""
Script to run a RabbitMQ producer service.
Used as the entry point for the producer Docker container.
"""
import os
import time
import uuid
import random
import json
import signal
import threading
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Make sure we can import from the main project
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from prometheus_client import start_http_server
    from rabbitmq_project.producers.order_producer import OrderProducer
    from rabbitmq_project.services.monitoring import monitoring_service
except ImportError as e:
    logger.error(f"Import error: {e}")
    sys.exit(1)

# Configuration from environment variables
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'admin123')
PRODUCER_TYPE = os.environ.get('PRODUCER_TYPE', 'order')
ORDER_RATE = float(os.environ.get('ORDER_RATE', 1.0))  # Orders per second
METRICS_PORT = int(os.environ.get('METRICS_PORT', 9091))

# Global flag to control the main loop
running = True

def generate_order():
    """Generate a random order."""
    customer_types = ['standard', 'premium', 'business']
    products = [
        {'id': 'product1', 'name': 'Product 1', 'price': 10.99},
        {'id': 'product2', 'name': 'Product 2', 'price': 24.99},
        {'id': 'product3', 'name': 'Product 3', 'price': 5.49},
        {'id': 'product4', 'name': 'Product 4', 'price': 99.99}
    ]
    
    # Generate random order data
    order_id = f"order-{uuid.uuid4()}"
    customer_id = f"customer-{random.randint(1000, 9999)}"
    customer_type = random.choice(customer_types)
    
    # Add between 1 and 5 random items
    num_items = random.randint(1, 5)
    items = []
    
    for _ in range(num_items):
        product = random.choice(products)
        quantity = random.randint(1, 3)
        items.append({
            'product_id': product['id'],
            'product_name': product['name'],
            'price': product['price'],
            'quantity': quantity
        })
    
    # Calculate totals
    subtotal = sum(item['price'] * item['quantity'] for item in items)
    tax = subtotal * 0.08
    total = subtotal + tax
    
    # Create the order
    order = {
        'id': order_id,
        'customer_id': customer_id,
        'customer_type': customer_type,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'items': items,
        'subtotal': round(subtotal, 2),
        'tax': round(tax, 2),
        'total': round(total, 2),
        'status': 'created'
    }
    
    return order

def run_order_producer():
    """Run the order producer service."""
    logger.info(f"Starting order producer connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    try:
        # Create an order producer
        producer = OrderProducer()
        
        while running:
            try:
                # Generate a random order
                order = generate_order()
                logger.info(f"Publishing order {order['id']} for customer type {order['customer_type']}")
                
                # Publish the order
                producer.publish_order_created(order)
                
                # Randomly publish updates or cancellations
                if random.random() < 0.2:  # 20% chance of an update
                    time.sleep(random.uniform(0.5, 2.0))
                    updates = {
                        'status': random.choice(['processing', 'shipped', 'delivered']),
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }
                    producer.publish_order_updated(order['id'], updates)
                    logger.info(f"Published update for order {order['id']}: {updates['status']}")
                    
                elif random.random() < 0.1:  # 10% chance of cancellation
                    time.sleep(random.uniform(0.5, 2.0))
                    reason = random.choice([
                        'customer_request', 
                        'payment_failed', 
                        'out_of_stock',
                        'shipping_unavailable'
                    ])
                    producer.publish_order_cancelled(order['id'], reason)
                    logger.info(f"Published cancellation for order {order['id']}: {reason}")
                
                # Occasionally broadcast a notification
                if random.random() < 0.05:  # 5% chance
                    notification = {
                        'message': random.choice([
                            'System maintenance scheduled',
                            'New shipping options available',
                            'Price updates in progress',
                            'Holiday schedule in effect'
                        ]),
                        'level': random.choice(['info', 'warning']),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    producer.broadcast_order_notification(notification)
                    logger.info(f"Broadcast notification: {notification['message']}")
                
                # Wait for next order
                sleep_time = 1.0 / ORDER_RATE if ORDER_RATE > 0 else 1.0
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in order producer main loop: {e}")
                # Wait before retrying
                time.sleep(5)
                
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
    finally:
        logger.info("Order producer stopping")

def signal_handler(sig, frame):
    """Handle termination signals."""
    global running
    logger.info(f"Received signal {sig}, shutting down...")
    running = False

def main():
    """Main entry point."""
    global running
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start Prometheus metrics server
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Started Prometheus metrics server on port {METRICS_PORT}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
    
    # Start monitoring service
    try:
        monitoring_service.rabbitmq_hosts = [RABBITMQ_HOST]
        monitoring_service.start()
        logger.info("Started RabbitMQ monitoring service")
    except Exception as e:
        logger.error(f"Failed to start monitoring service: {e}")
    
    # Start the producer based on type
    if PRODUCER_TYPE == 'order':
        producer_thread = threading.Thread(target=run_order_producer)
        producer_thread.daemon = True
        producer_thread.start()
        
        # Wait for termination
        while running:
            time.sleep(1)
            
        # Wait for thread to finish
        producer_thread.join(timeout=5.0)
    else:
        logger.error(f"Unknown producer type: {PRODUCER_TYPE}")
        sys.exit(1)
    
    # Stop monitoring service
    try:
        monitoring_service.stop()
        logger.info("Stopped RabbitMQ monitoring service")
    except Exception as e:
        logger.error(f"Error stopping monitoring service: {e}")
    
    logger.info("Producer service shutdown complete")

if __name__ == '__main__':
    main()