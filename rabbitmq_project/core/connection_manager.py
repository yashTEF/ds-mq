"""
Connection manager module for handling RabbitMQ connections.
Provides resilient connection handling with automatic reconnection.
"""
import time
import pika
import threading
from ..config.config import config
from .logger import get_logger

# Module logger
logger = get_logger(__name__)

# Thread-local storage for connections
_thread_local = threading.local()

class ConnectionManager:
    """
    Manages RabbitMQ connections with automatic reconnection on failure.
    """
    def __init__(self, host=None, port=None, username=None, password=None, vhost=None):
        """
        Initialize a new connection manager.
        
        Args:
            host: RabbitMQ host (default: from config)
            port: RabbitMQ port (default: from config)
            username: RabbitMQ username (default: from config)
            password: RabbitMQ password (default: from config)
            vhost: RabbitMQ virtual host (default: from config)
        """
        self.host = host or config.RABBITMQ_HOST
        self.port = port or config.RABBITMQ_PORT
        self.username = username or config.RABBITMQ_USER
        self.password = password or config.RABBITMQ_PASSWORD
        self.vhost = vhost or config.RABBITMQ_VHOST
        
        self._connection = None
        self._lock = threading.RLock()
        
    def _create_connection(self):
        """
        Create a new connection to RabbitMQ.
        
        Returns:
            A new pika.BlockingConnection
        """
        logger.info(f"Creating new connection to {self.host}:{self.port}/{self.vhost}")
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
            # Additional parameters for better reliability
            heartbeat=60,  # Heartbeat every 60 seconds
            blocked_connection_timeout=300,  # Timeout for blocked connections
            connection_attempts=3,  # Default connection attempts
            retry_delay=5  # Delay between attempts
        )
        
        return pika.BlockingConnection(parameters)

    def get_connection(self):
        """
        Get a connection to RabbitMQ, creating a new one if needed.
        Uses thread-local storage to maintain one connection per thread.
        
        Returns:
            A pika.BlockingConnection
        """
        # Check for existing thread-local connection
        if hasattr(_thread_local, 'connection') and _thread_local.connection:
            # Check if connection is still open
            if _thread_local.connection.is_open:
                return _thread_local.connection
        
        # Create new connection with retry
        with self._lock:
            for retry in range(config.CONNECTION_MAX_RETRIES):
                try:
                    _thread_local.connection = self._create_connection()
                    return _thread_local.connection
                except Exception as e:
                    logger.warning(f"Connection attempt {retry+1} failed: {e}")
                    if retry < config.CONNECTION_MAX_RETRIES - 1:
                        delay = config.CONNECTION_RETRY_DELAY * (2 ** retry)  # Exponential backoff
                        logger.info(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed to connect after {config.CONNECTION_MAX_RETRIES} attempts")
                        raise ConnectionError(f"Could not connect to RabbitMQ at {self.host}:{self.port}") from e

    def close_connection(self):
        """
        Close the current thread's connection if it exists.
        """
        if hasattr(_thread_local, 'connection') and _thread_local.connection:
            try:
                logger.info("Closing connection")
                _thread_local.connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                _thread_local.connection = None

# Singleton instance
connection_manager = ConnectionManager()

def get_connection():
    """
    Get a connection from the default connection manager.
    
    Returns:
        A pika.BlockingConnection
    """
    return connection_manager.get_connection()

def close_connection():
    """
    Close the current thread's connection.
    """
    connection_manager.close_connection()