#!/usr/bin/env python
"""
Script to monitor and log all messages passing through RabbitMQ.
Connects to the firehose exchange to capture all message traffic.
"""
import os
import json
import time
import logging
import pika
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/message_events.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MessageLogger:
    """Logs all messages passing through RabbitMQ via the firehose feature."""
    
    def __init__(self):
        # RabbitMQ connection parameters
        self.host = os.environ.get('RABBITMQ_HOST', 'localhost')
        self.port = int(os.environ.get('RABBITMQ_PORT', 5672))
        self.user = os.environ.get('RABBITMQ_USER', 'admin')
        self.password = os.environ.get('RABBITMQ_PASSWORD', 'admin123')
        
        # Ensure logs directory exists
        os.makedirs("logs", exist_ok=True)
        self.log_file = f"logs/rabbitmq_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        logger.info(f"Message logs will be saved to: {self.log_file}")
        
    def setup_connection(self):
        """Set up connection to RabbitMQ"""
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host='/',
            credentials=credentials,
            heartbeat=30,
            connection_attempts=3,
            retry_delay=2
        )
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        logger.info("Connected to RabbitMQ successfully")
        
        return connection, channel
        
    def setup_firehose(self):
        """Set up connection to the RabbitMQ firehose exchange"""
        try:
            connection, channel = self.setup_connection()
            
            # Create a queue for the firehose
            result = channel.queue_declare(queue='message_logger_queue', exclusive=True)
            queue_name = result.method.queue
            
            # Bind to the firehose exchange
            channel.queue_bind(
                exchange='amq.rabbitmq.trace',
                queue=queue_name,
                routing_key='#'
            )
            
            logger.info(f"Listening to all messages via firehose exchange")
            
            # Set up a consumer
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=self.log_message,
                auto_ack=True
            )
            
            try:
                logger.info("Starting to consume messages... Press CTRL+C to exit")
                channel.start_consuming()
            except KeyboardInterrupt:
                logger.info("Interrupted by user, shutting down...")
            finally:
                channel.stop_consuming()
                connection.close()
                
        except Exception as e:
            logger.error(f"Error setting up firehose: {e}")
            
    def log_message(self, ch, method, properties, body):
        """Log each message that passes through RabbitMQ"""
        try:
            # Try to parse JSON content
            try:
                content = json.loads(body)
                content_type = "json"
            except json.JSONDecodeError:
                content = body.decode('utf-8', errors='replace')
                content_type = "text"
            
            # Create log entry with detailed information
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "exchange": method.exchange,
                "routing_key": method.routing_key,
                "content_type": content_type,
                "content": content,
                "properties": {
                    "content_type": properties.content_type,
                    "headers": properties.headers,
                    "delivery_mode": properties.delivery_mode,
                    "message_id": properties.message_id,
                    "timestamp": properties.timestamp
                } if properties else {}
            }
            
            # Write to log file
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
                
            # Log summary to console
            exchange = method.exchange or "default"
            routing_key = method.routing_key or "none"
            logger.info(f"Message: {exchange} -> {routing_key} [{content_type}]")
            
        except Exception as e:
            logger.error(f"Error logging message: {e}")

if __name__ == "__main__":
    logger.info("Starting RabbitMQ Message Event Logger")
    message_logger = MessageLogger()
    message_logger.setup_firehose()