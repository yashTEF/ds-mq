"""
Configuration module for RabbitMQ settings.
Loads and validates environment variables with defaults.
"""
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Central configuration class for RabbitMQ settings."""
    
    # RabbitMQ connection settings
    RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
    RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
    RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
    RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
    RABBITMQ_VHOST = os.getenv('RABBITMQ_VHOST', '/')
    
    # Connection retry settings
    CONNECTION_RETRY_DELAY = int(os.getenv('CONNECTION_RETRY_DELAY', '5'))  # seconds
    CONNECTION_MAX_RETRIES = int(os.getenv('CONNECTION_MAX_RETRIES', '5'))
    
    # Consumer retry settings
    CONSUMER_RETRY_DELAY = int(os.getenv('CONSUMER_RETRY_DELAY', '5'))  # seconds
    CONSUMER_MAX_RETRIES = int(os.getenv('CONSUMER_MAX_RETRIES', '3'))
    CONSUMER_RETRY_BACKOFF_FACTOR = float(os.getenv('CONSUMER_RETRY_BACKOFF_FACTOR', '2.0'))
    
    # Queue settings
    DEFAULT_QUEUE_DURABLE = os.getenv('DEFAULT_QUEUE_DURABLE', 'True').lower() in ('true', 'yes', '1')
    DEFAULT_EXCHANGE_DURABLE = os.getenv('DEFAULT_EXCHANGE_DURABLE', 'True').lower() in ('true', 'yes', '1')
    
    # Monitoring settings
    METRICS_PORT = int(os.getenv('METRICS_PORT', '8000'))
    ENABLE_METRICS = os.getenv('ENABLE_METRICS', 'True').lower() in ('true', 'yes', '1')
    
    # Logging settings
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = os.getenv('LOG_FORMAT', '%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/rabbitmq.log')
    
    # Environment (dev, test, prod)
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()
    
    @classmethod
    def validate(cls):
        """Validate that all required configuration is present and valid."""
        valid = True
        
        # Check for required settings
        if not cls.RABBITMQ_HOST:
            logging.error("RABBITMQ_HOST is not set!")
            valid = False
            
        # Validate integer values
        try:
            int(cls.RABBITMQ_PORT)
        except ValueError:
            logging.error(f"RABBITMQ_PORT is not a valid integer: {cls.RABBITMQ_PORT}")
            valid = False
            
        # Check credentials for production
        if cls.ENVIRONMENT == 'production' and (cls.RABBITMQ_USER == 'guest' or cls.RABBITMQ_PASSWORD == 'guest'):
            logging.warning("Using default RabbitMQ credentials in production is not recommended!")
            
        return valid

# Create a singleton configuration object
config = Config()

# Validate configuration on import
if not config.validate():
    logging.warning("Configuration validation failed, using defaults where possible.")