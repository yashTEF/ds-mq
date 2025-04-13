"""
Channel manager module for handling RabbitMQ channels.
Creates and manages channels on top of connections from the connection manager.
"""
import threading
from .connection_manager import get_connection
from .logger import get_logger

# Module logger
logger = get_logger(__name__)

# Thread-local storage for channels
_thread_local = threading.local()

class ChannelManager:
    """
    Manages RabbitMQ channels with automatic channel creation.
    """
    
    def __init__(self):
        """Initialize a new channel manager."""
        self._lock = threading.RLock()
        
    def get_channel(self, prefetch_count=None):
        """
        Get a channel from the current connection, creating a new one if needed.
        Uses thread-local storage to maintain one channel per thread.
        
        Args:
            prefetch_count: Optional prefetch count for the channel (for fair dispatch)
            
        Returns:
            A pika.channel.Channel
        """
        # Check for existing thread-local channel
        if hasattr(_thread_local, 'channel') and _thread_local.channel:
            # Check if channel is still open
            try:
                if _thread_local.channel.is_open:
                    return _thread_local.channel
            except Exception:
                # If checking fails, create a new channel
                pass
        
        # Create new channel
        with self._lock:
            try:
                connection = get_connection()
                _thread_local.channel = connection.channel()
                logger.debug("Created new channel")
                
                # Set prefetch count if specified
                if prefetch_count is not None:
                    self.set_qos(prefetch_count)
                    
                return _thread_local.channel
            except Exception as e:
                logger.error(f"Failed to create channel: {e}")
                raise
    
    def set_qos(self, prefetch_count):
        """
        Set the Quality of Service (QoS) for the current channel.
        
        Args:
            prefetch_count: Number of messages to prefetch
        """
        if hasattr(_thread_local, 'channel') and _thread_local.channel:
            try:
                _thread_local.channel.basic_qos(prefetch_count=prefetch_count)
                logger.debug(f"Set prefetch count to {prefetch_count}")
            except Exception as e:
                logger.warning(f"Failed to set QoS: {e}")
    
    def close_channel(self):
        """
        Close the current thread's channel if it exists.
        """
        if hasattr(_thread_local, 'channel') and _thread_local.channel:
            try:
                logger.debug("Closing channel")
                _thread_local.channel.close()
            except Exception as e:
                logger.warning(f"Error closing channel: {e}")
            finally:
                _thread_local.channel = None

# Singleton instance
channel_manager = ChannelManager()

def get_channel(prefetch_count=None):
    """
    Get a channel from the default channel manager.
    
    Args:
        prefetch_count: Optional prefetch count for the channel
        
    Returns:
        A pika.channel.Channel
    """
    return channel_manager.get_channel(prefetch_count)

def close_channel():
    """
    Close the current thread's channel.
    """
    channel_manager.close_channel()