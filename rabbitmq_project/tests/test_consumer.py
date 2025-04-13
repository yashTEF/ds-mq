"""
Unit tests for the consumer components.
Tests the functionality of the BaseConsumer and InventoryConsumer classes.
"""
import unittest
from unittest.mock import MagicMock, patch, call
import json
import pika
from ..consumers.base_consumer import BaseConsumer
from ..consumers.inventory_consumer import InventoryConsumer
from ..core.retry_handler import retry_handler

class TestBaseConsumer(unittest.TestCase):
    """Tests for the BaseConsumer class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock channel
        self.mock_channel = MagicMock()
        
        # Create mock connection
        self.mock_connection = MagicMock()
        self.mock_connection.channel.return_value = self.mock_channel
        
        # Patch the get_channel function to return our mock
        self.channel_patcher = patch('rabbitmq_project.consumers.base_consumer.get_channel')
        self.mock_get_channel = self.channel_patcher.start()
        self.mock_get_channel.return_value = self.mock_channel
        
        # Create a test consumer
        self.consumer = BaseConsumer(
            queue_name='test_queue',
            exchange_name='test_exchange',
            exchange_type='direct',
            routing_key='test_key'
        )
        
        # Create basic message props and delivery info
        self.mock_properties = MagicMock()
        self.mock_properties.content_type = 'application/json'
        
        self.mock_delivery = MagicMock()
        self.mock_delivery.delivery_tag = 123
        self.mock_delivery.routing_key = 'test_key'
        
    def tearDown(self):
        """Clean up after each test."""
        self.channel_patcher.stop()
        
    def test_init(self):
        """Test initializing a consumer."""
        self.assertEqual(self.consumer.queue_name, 'test_queue')
        self.assertEqual(self.consumer.exchange_name, 'test_exchange')
        self.assertEqual(self.consumer.exchange_type, 'direct')
        self.assertEqual(self.consumer.routing_key, 'test_key')
        self.assertEqual(self.consumer.prefetch_count, 1)
        self.assertFalse(self.consumer.auto_ack)
        self.assertTrue(self.consumer.declare_queue)
        self.assertTrue(self.consumer.durable_queue)
        self.assertFalse(self.consumer.exclusive_queue)
        self.assertFalse(self.consumer.auto_delete_queue)
        
    def test_setup_channel_declares_queue_and_exchange(self):
        """Test that _setup_channel declares the queue and exchange."""
        channel = self.consumer._setup_channel()
        
        # Verify QoS was set
        self.mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        
        # Verify queue was declared
        self.mock_channel.queue_declare.assert_called_once_with(
            queue='test_queue',
            durable=True,
            exclusive=False,
            auto_delete=False
        )
        
        # Verify exchange was declared
        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange='test_exchange',
            exchange_type='direct',
            durable=True
        )
        
        # Verify queue was bound to exchange
        self.mock_channel.queue_bind.assert_called_once_with(
            queue='test_queue',
            exchange='test_exchange',
            routing_key='test_key'
        )
        
        # Check return value
        self.assertEqual(channel, self.mock_channel)
        
    def test_process_message_json(self):
        """Test processing a JSON message."""
        # Create a spy on handle_message
        self.consumer.handle_message = MagicMock(return_value=True)
        
        # Create a JSON message
        message_dict = {'key': 'value', 'number': 42}
        message_body = json.dumps(message_dict).encode('utf-8')
        
        # Process the message
        self.consumer._process_message(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            message_body
        )
        
        # Verify handle_message was called with parsed JSON
        self.consumer.handle_message.assert_called_once_with(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            message_dict
        )
        
        # Verify message was acknowledged
        self.mock_channel.basic_ack.assert_called_once_with(
            delivery_tag=self.mock_delivery.delivery_tag
        )
        
    def test_process_message_text(self):
        """Test processing a text message."""
        # Create a spy on handle_message
        self.consumer.handle_message = MagicMock(return_value=True)
        
        # Set content type to text
        self.mock_properties.content_type = 'text/plain'
        
        # Create a text message
        message_text = "Hello, world!"
        message_body = message_text.encode('utf-8')
        
        # Process the message
        self.consumer._process_message(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            message_body
        )
        
        # Verify handle_message was called with text
        self.consumer.handle_message.assert_called_once_with(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            message_text
        )
        
    def test_process_message_failed_handling(self):
        """Test processing a message where handling fails."""
        # Set up retry_handler as mock
        retry_patch = patch('rabbitmq_project.consumers.base_consumer.retry_handler')
        mock_retry = retry_patch.start()
        
        try:
            # Create a spy on handle_message that returns False (indicating failure)
            self.consumer.handle_message = MagicMock(return_value=False)
            
            # Create a message
            message_body = b'test message'
            
            # Process the message
            self.consumer._process_message(
                self.mock_channel,
                self.mock_delivery,
                self.mock_properties,
                message_body
            )
            
            # Verify retry_handler was called
            mock_retry.process_retry.assert_called_once_with(
                self.mock_channel,
                self.mock_delivery,
                self.mock_properties,
                message_body,
                queue_name='test_queue'
            )
            
            # Verify message was not acknowledged
            self.mock_channel.basic_ack.assert_not_called()
            
        finally:
            retry_patch.stop()
            
    def test_process_message_exception(self):
        """Test processing a message where an exception occurs."""
        # Set up retry_handler as mock
        retry_patch = patch('rabbitmq_project.consumers.base_consumer.retry_handler')
        mock_retry = retry_patch.start()
        
        try:
            # Create a spy on handle_message that raises an exception
            self.consumer.handle_message = MagicMock(side_effect=Exception("Test exception"))
            
            # Create a message
            message_body = b'test message'
            
            # Process the message
            self.consumer._process_message(
                self.mock_channel,
                self.mock_delivery,
                self.mock_properties,
                message_body
            )
            
            # Verify retry_handler was called with the exception
            mock_retry.process_retry.assert_called_once()
            args = mock_retry.process_retry.call_args[0]
            self.assertEqual(args[0], self.mock_channel)
            self.assertEqual(args[1], self.mock_delivery)
            self.assertEqual(args[2], self.mock_properties)
            self.assertEqual(args[3], message_body)
            
            # Check exception was passed
            kwargs = mock_retry.process_retry.call_args[1]
            self.assertIsInstance(kwargs['exception'], Exception)
            self.assertEqual(str(kwargs['exception']), "Test exception")
            
            # Verify message was not acknowledged
            self.mock_channel.basic_ack.assert_not_called()
            
        finally:
            retry_patch.stop()
            
    def test_start_consuming(self):
        """Test starting consumer in non-blocking mode."""
        # Mock the consume thread
        with patch('rabbitmq_project.consumers.base_consumer.threading.Thread') as mock_thread:
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance
            
            # Start consuming in non-blocking mode
            self.consumer.start_consuming(block=False)
            
            # Verify consumer was set up
            self.mock_channel.basic_consume.assert_called_once_with(
                queue='test_queue',
                on_message_callback=self.consumer._process_message,
                auto_ack=False
            )
            
            # Verify thread was started
            mock_thread.assert_called_once()
            mock_thread_instance.start.assert_called_once()
            
            # Check state
            self.assertTrue(self.consumer._consuming)
            self.assertFalse(self.consumer._stop_event.is_set())
            
    def test_stop_consuming(self):
        """Test stopping a consumer."""
        # Set up consumer as if it's running
        self.consumer._consuming = True
        self.consumer._consumer_tag = 'test_consumer_tag'
        self.consumer._channel = self.mock_channel
        
        # Stop consumer
        self.consumer.stop_consuming()
        
        # Verify consumer was cancelled
        self.mock_channel.basic_cancel.assert_called_once_with('test_consumer_tag')
        
        # Verify channel was stopped
        self.mock_channel.stop_consuming.assert_called_once()
        
        # Check state
        self.assertFalse(self.consumer._consuming)
        self.assertIsNone(self.consumer._consumer_tag)
        
    def test_close(self):
        """Test closing a consumer."""
        # Spy on stop_consuming
        self.consumer.stop_consuming = MagicMock()
        
        # Close consumer
        self.consumer.close()
        
        # Verify stop_consuming was called
        self.consumer.stop_consuming.assert_called_once()
        
    def test_context_manager(self):
        """Test using consumer as a context manager."""
        # Spy on close
        self.consumer.close = MagicMock()
        
        # Use as context manager
        with self.consumer as c:
            # Check consumer was returned
            self.assertEqual(c, self.consumer)
            
        # Verify close was called
        self.consumer.close.assert_called_once()

class TestInventoryConsumer(unittest.TestCase):
    """Tests for the InventoryConsumer class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock channel
        self.mock_channel = MagicMock()
        
        # Patch the get_channel function to return our mock
        self.channel_patcher = patch('rabbitmq_project.consumers.base_consumer.get_channel')
        self.mock_get_channel = self.channel_patcher.start()
        self.mock_get_channel.return_value = self.mock_channel
        
        # Create a test inventory consumer
        self.inventory_consumer = InventoryConsumer()
        
        # Create basic message props and delivery info
        self.mock_properties = MagicMock()
        self.mock_properties.content_type = 'application/json'
        
        self.mock_delivery = MagicMock()
        self.mock_delivery.delivery_tag = 123
        
    def tearDown(self):
        """Clean up after each test."""
        self.channel_patcher.stop()
        
    def test_init(self):
        """Test initializing an inventory consumer."""
        self.assertEqual(self.inventory_consumer.queue_name, 'inventory')
        self.assertEqual(self.inventory_consumer.exchange_name, 'orders')
        self.assertEqual(self.inventory_consumer.exchange_type, 'topic')
        self.assertEqual(self.inventory_consumer.routing_key, 'order.created.*')
        
        # Check inventory was initialized
        self.assertIsNotNone(self.inventory_consumer.inventory)
        self.assertGreater(len(self.inventory_consumer.inventory), 0)
        
    def test_get_inventory_status(self):
        """Test checking inventory status."""
        # Check status of existing product
        product, quantity, in_stock = self.inventory_consumer.get_inventory_status('product1')
        self.assertIsNotNone(product)
        self.assertEqual(product['name'], 'Product 1')
        self.assertEqual(quantity, 100)
        self.assertTrue(in_stock)
        
        # Check status of out-of-stock product
        product, quantity, in_stock = self.inventory_consumer.get_inventory_status('product5')
        self.assertIsNotNone(product)
        self.assertEqual(quantity, 0)
        self.assertFalse(in_stock)
        
        # Check status of non-existent product
        product, quantity, in_stock = self.inventory_consumer.get_inventory_status('nonexistent')
        self.assertIsNone(product)
        self.assertEqual(quantity, 0)
        self.assertFalse(in_stock)
        
    def test_update_inventory(self):
        """Test updating inventory."""
        # Successful update (decrement)
        result = self.inventory_consumer.update_inventory('product1', -10)
        self.assertTrue(result)
        self.assertEqual(self.inventory_consumer.inventory['product1']['quantity'], 90)
        
        # Successful update (increment)
        result = self.inventory_consumer.update_inventory('product1', 5)
        self.assertTrue(result)
        self.assertEqual(self.inventory_consumer.inventory['product1']['quantity'], 95)
        
        # Insufficient inventory
        result = self.inventory_consumer.update_inventory('product1', -100)
        self.assertFalse(result)
        self.assertEqual(self.inventory_consumer.inventory['product1']['quantity'], 95)
        
        # Non-existent product
        result = self.inventory_consumer.update_inventory('nonexistent', -1)
        self.assertFalse(result)
        
    def test_process_order_created(self):
        """Test processing an order created event."""
        # Create a test order
        order_data = {
            'id': 'order123',
            'customer_id': 'customer456',
            'items': [
                {'product_id': 'product1', 'quantity': 5},
                {'product_id': 'product2', 'quantity': 3}
            ]
        }
        
        # Get initial inventory levels
        initial_product1 = self.inventory_consumer.inventory['product1']['quantity']
        initial_product2 = self.inventory_consumer.inventory['product2']['quantity']
        
        # Process the order
        result = self.inventory_consumer.process_order_created(order_data)
        
        # Verify result
        self.assertTrue(result)
        
        # Check inventory was updated
        self.assertEqual(
            self.inventory_consumer.inventory['product1']['quantity'],
            initial_product1 - 5
        )
        self.assertEqual(
            self.inventory_consumer.inventory['product2']['quantity'],
            initial_product2 - 3
        )
        
    def test_process_order_created_partial_availability(self):
        """Test processing an order with partial availability."""
        # Create a test order with one unavailable product
        order_data = {
            'id': 'order123',
            'customer_id': 'customer456',
            'items': [
                {'product_id': 'product1', 'quantity': 5},
                {'product_id': 'product5', 'quantity': 1}  # Out of stock
            ]
        }
        
        # Get initial inventory level
        initial_product1 = self.inventory_consumer.inventory['product1']['quantity']
        
        # Process the order
        result = self.inventory_consumer.process_order_created(order_data)
        
        # Verify result
        self.assertTrue(result)
        
        # Check inventory was updated for available product
        self.assertEqual(
            self.inventory_consumer.inventory['product1']['quantity'],
            initial_product1 - 5
        )
        
        # Check out-of-stock product remains unchanged
        self.assertEqual(
            self.inventory_consumer.inventory['product5']['quantity'],
            0
        )
        
    def test_handle_message_order_created(self):
        """Test handling an order created message."""
        # Spy on process_order_created
        self.inventory_consumer.process_order_created = MagicMock(return_value=True)
        
        # Create a message for order.created routing key
        self.mock_delivery.routing_key = 'order.created.standard'
        
        # Create message body
        order_data = {
            'id': 'order123',
            'customer_id': 'customer456',
            'items': [{'product_id': 'product1', 'quantity': 1}]
        }
        message_body = {
            'event_type': 'created',
            'data': order_data
        }
        
        # Handle the message
        with patch('random.random', return_value=0.9):  # Ensure no simulated error
            result = self.inventory_consumer.handle_message(
                self.mock_channel,
                self.mock_delivery,
                self.mock_properties,
                message_body
            )
        
        # Verify result
        self.assertTrue(result)
        
        # Verify process_order_created was called
        self.inventory_consumer.process_order_created.assert_called_once_with(order_data)
        
    def test_handle_message_simulated_error(self):
        """Test handling a message with simulated error."""
        # Create a message
        self.mock_delivery.routing_key = 'order.created.standard'
        message_body = {'data': {}}
        
        # Handle the message with simulated error (random < 0.2)
        with patch('random.random', return_value=0.1):
            result = self.inventory_consumer.handle_message(
                self.mock_channel,
                self.mock_delivery,
                self.mock_properties,
                message_body
            )
        
        # Verify result
        self.assertFalse(result)
        
if __name__ == '__main__':
    unittest.main()