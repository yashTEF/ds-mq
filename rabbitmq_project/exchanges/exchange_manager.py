"""
Exchange manager for declaring and managing RabbitMQ exchanges.
Supports direct, topic, fanout, and headers exchange types.
"""
from ..core.channel_manager import get_channel
from ..core.logger import get_logger
from ..config.config import config

# Module logger
logger = get_logger(__name__)

class ExchangeManager:
    """
    Manages RabbitMQ exchanges.
    Provides methods to declare and delete exchanges of different types.
    """
    
    def __init__(self):
        """Initialize exchange manager."""
        pass
        
    def declare_exchange(self, exchange_name, exchange_type='direct', 
                         durable=None, auto_delete=False, 
                         internal=False, arguments=None, channel=None):
        """
        Declare a RabbitMQ exchange.
        
        Args:
            exchange_name: Name of the exchange
            exchange_type: Type of exchange ('direct', 'topic', 'fanout', 'headers')
            durable: Whether the exchange survives broker restarts
            auto_delete: Whether to delete the exchange when no queues are bound to it
            internal: Whether the exchange is internal (used for exchange-to-exchange binding)
            arguments: Additional exchange arguments
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if exchange declared successfully
        """
        if durable is None:
            durable = config.DEFAULT_EXCHANGE_DURABLE
            
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Declaring {exchange_type} exchange '{exchange_name}' (durable={durable})")
            
            channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                arguments=arguments
            )
            
            logger.debug(f"Exchange '{exchange_name}' declared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to declare exchange '{exchange_name}': {e}")
            raise
            
    def delete_exchange(self, exchange_name, if_unused=False, channel=None):
        """
        Delete a RabbitMQ exchange.
        
        Args:
            exchange_name: Name of the exchange to delete
            if_unused: Only delete if the exchange has no bindings
            channel: Optional channel to use, otherwise gets one
            
        Returns:
            True if exchange deleted successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Deleting exchange '{exchange_name}'")
            
            channel.exchange_delete(
                exchange=exchange_name,
                if_unused=if_unused
            )
            
            logger.debug(f"Exchange '{exchange_name}' deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete exchange '{exchange_name}': {e}")
            raise
            
    def declare_direct_exchange(self, exchange_name, durable=None, channel=None):
        """
        Declare a direct exchange.
        
        Args:
            exchange_name: Name of the exchange
            durable: Whether the exchange survives broker restarts
            channel: Optional channel to use
            
        Returns:
            True if exchange declared successfully
        """
        return self.declare_exchange(
            exchange_name, 
            exchange_type='direct',
            durable=durable,
            channel=channel
        )
        
    def declare_topic_exchange(self, exchange_name, durable=None, channel=None):
        """
        Declare a topic exchange.
        
        Args:
            exchange_name: Name of the exchange
            durable: Whether the exchange survives broker restarts
            channel: Optional channel to use
            
        Returns:
            True if exchange declared successfully
        """
        return self.declare_exchange(
            exchange_name, 
            exchange_type='topic',
            durable=durable,
            channel=channel
        )
        
    def declare_fanout_exchange(self, exchange_name, durable=None, channel=None):
        """
        Declare a fanout exchange.
        
        Args:
            exchange_name: Name of the exchange
            durable: Whether the exchange survives broker restarts
            channel: Optional channel to use
            
        Returns:
            True if exchange declared successfully
        """
        return self.declare_exchange(
            exchange_name, 
            exchange_type='fanout',
            durable=durable,
            channel=channel
        )
        
    def declare_headers_exchange(self, exchange_name, durable=None, channel=None):
        """
        Declare a headers exchange.
        
        Args:
            exchange_name: Name of the exchange
            durable: Whether the exchange survives broker restarts
            channel: Optional channel to use
            
        Returns:
            True if exchange declared successfully
        """
        return self.declare_exchange(
            exchange_name, 
            exchange_type='headers',
            durable=durable,
            channel=channel
        )
        
    def bind_exchange(self, source, destination, routing_key="", 
                      arguments=None, channel=None):
        """
        Bind an exchange to another exchange.
        
        Args:
            source: Source exchange name
            destination: Destination exchange name
            routing_key: Routing key for the binding
            arguments: Additional binding arguments
            channel: Optional channel to use
            
        Returns:
            True if binding created successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Binding exchange '{source}' to '{destination}' with key '{routing_key}'")
            
            channel.exchange_bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments
            )
            
            logger.debug(f"Exchange binding from '{source}' to '{destination}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to bind exchanges '{source}' to '{destination}': {e}")
            raise
            
    def unbind_exchange(self, source, destination, routing_key="",
                        arguments=None, channel=None):
        """
        Unbind an exchange from another exchange.
        
        Args:
            source: Source exchange name
            destination: Destination exchange name
            routing_key: Routing key for the binding
            arguments: Additional binding arguments
            channel: Optional channel to use
            
        Returns:
            True if binding removed successfully
        """
        try:
            if channel is None:
                channel = get_channel()
                
            logger.info(f"Unbinding exchange '{source}' from '{destination}' with key '{routing_key}'")
            
            channel.exchange_unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments
            )
            
            logger.debug(f"Exchange binding from '{source}' to '{destination}' removed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unbind exchanges '{source}' from '{destination}': {e}")
            raise

# Singleton instance
exchange_manager = ExchangeManager()