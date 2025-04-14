#!/usr/bin/env python
"""
Script to run a RabbitMQ consumer service.
Used as the entry point for the consumer Docker container.
"""
import os
import time
import signal
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

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
    from rabbitmq_project.consumers.inventory_consumer import InventoryConsumer
    from rabbitmq_project.services.monitoring import monitoring_service
except ImportError as e:
    logger.error(f"Import error: {e}")
    sys.exit(1)

# Configuration from environment variables
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'admin123')
CONSUMER_TYPE = os.environ.get('CONSUMER_TYPE', 'inventory')
CONSUMER_INSTANCES = int(os.environ.get('CONSUMER_INSTANCES', 1))
PREFETCH_COUNT = int(os.environ.get('PREFETCH_COUNT', 1))
METRICS_PORT = int(os.environ.get('METRICS_PORT', 9091))

# Global flag to control the main loop
running = True

# List to keep track of consumer instances
consumers = []

def run_inventory_consumer(consumer_id):
    """Run a single inventory consumer instance."""
    logger.info(f"Starting inventory consumer {consumer_id} connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    
    try:
        # Create an inventory consumer with specific prefetch count
        consumer = InventoryConsumer(prefetch_count=PREFETCH_COUNT)
        consumers.append(consumer)
        
        # Start consuming messages
        consumer.start_consuming(block=True)
        
    except Exception as e:
        logger.error(f"Error in inventory consumer {consumer_id}: {e}")
    finally:
        logger.info(f"Inventory consumer {consumer_id} stopping")

def signal_handler(sig, frame):
    """Handle termination signals."""
    global running
    logger.info(f"Received signal {sig}, shutting down...")
    running = False
    
    # Stop all consumers
    for consumer in consumers:
        try:
            consumer.stop_consuming()
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")

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
    
    # Start consumer threads based on type
    if CONSUMER_TYPE == 'inventory':
        # Create a thread pool for consumers
        with ThreadPoolExecutor(max_workers=CONSUMER_INSTANCES) as executor:
            # Submit consumer tasks
            futures = [
                executor.submit(run_inventory_consumer, i)
                for i in range(CONSUMER_INSTANCES)
            ]
            
            # Wait for termination signal
            while running:
                time.sleep(1)
            
            logger.info("Shutting down consumer threads...")
            
            # Wait for threads to finish (they will be interrupted by signal handler)
            for future in futures:
                try:
                    future.result(timeout=5.0)
                except Exception:
                    pass
    else:
        logger.error(f"Unknown consumer type: {CONSUMER_TYPE}")
        running = False
    
    # Stop monitoring service
    try:
        monitoring_service.stop()
        logger.info("Stopped RabbitMQ monitoring service")
    except Exception as e:
        logger.error(f"Error stopping monitoring service: {e}")
    
    logger.info("Consumer service shutdown complete")

if __name__ == '__main__':
    main()