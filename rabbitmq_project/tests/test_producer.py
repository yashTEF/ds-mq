"""
Unit tests for the producer components.
Tests the functionality of the BaseProducer and OrderProducer classes.
"""
import unittest
import json
from unittest.mock import MagicMock, patch
import pika
from ..producers.base_producer import BaseProducer
from ..producers.order_producer import OrderProducer

class TestBaseProducer(unittest.TestCase):
    """Tests for the BaseProducer class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock channel
        self.mock_channel = MagicMock()
        
        # Create mock connection
        self.mock_connection = MagicMock()
        self.mock_connection.channel.return_value = self.mock_channel
        
        # Patch the get_channel function to return our mock
        self.channel_patcher = patch('rabbitmq_project.producers.base_producer.get_channel')
        self.mock_get_channel = self.channel_patcher.start()
        self.mock_get_channel.return_value = self.mock_channel
        
        # Create a test producer
        self.producer = BaseProducer(
            exchange_name='test_exchange',
            exchange_type='direct',
            routing_key='test_key'
        )
        
    def tearDown(self):
        """Clean up after each test."""
        self.channel_patcher.stop()
        
    def test_init(self):
        """Test initializing a producer."""
        self.assertEqual(self.producer.exchange_name, 'test_exchange')
        self.assertEqual(self.producer.exchange_type, 'direct')
        self.assertEqual(self.producer.routing_key, 'test_key')
        self.assertTrue(self.producer.declare_exchange)
        self.assertFalse(self.producer.use_publisher_confirms)
        
    def test_get_channel(self):
        """Test getting a channel."""
        channel = self.producer.get_channel()
        self.assertEqual(channel, self.mock_channel)
        self.mock_get_channel.assert_called_once()
        
    def test_setup_channel_declares_exchange(self):
        """Test that _setup_channel declares the exchange."""
        self.producer._setup_channel()
        
        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange='test_exchange',
            exchange_type='direct',
            durable=True
        )
        
    def test_publish_string_message(self):
        """Test publishing a string message."""
        self.mock_channel.basic_publish.return_value = None
        
        # Publish a string message
        result = self.producer.publish('test message')
        
        # Check the result
        self.assertTrue(result)
        
        # Verify basic_publish was called with correct arguments
        self.mock_channel.basic_publish.assert_called_once()
        args, kwargs = self.mock_channel.basic_publish.call_args
        
        self.assertEqual(kwargs['exchange'], 'test_exchange')
        self.assertEqual(kwargs['routing_key'], 'test_key')
        self.assertEqual(kwargs['body'], b'test message')
        self.assertIsInstance(kwargs['properties'], pika.BasicProperties)
        self.assertEqual(kwargs['properties'].delivery_mode, 2)  # Persistent
        
    def test_publish_dict_message(self):
        """Test publishing a dict message (should be JSON-encoded)."""
        self.mock_channel.basic_publish.return_value = None
        
        # Publish a dict message
        test_dict = {'key': 'value', 'number': 42}
        result = self.producer.publish(test_dict)
        
        # Check the result
        self.assertTrue(result)
        
        # Verify basic_publish was called with correct arguments
        self.mock_channel.basic_publish.assert_called_once()
        args, kwargs = self.mock_channel.basic_publish.call_args
        
        # Check body was JSON-encoded
        body_json = json.loads(kwargs['body'].decode('utf-8'))
        self.assertEqual(body_json, test_dict)
        
    def test_publish_with_confirm(self):
        """Test publishing with publisher confirms."""
        # Create a producer with confirms enabled
        producer = BaseProducer(
            exchange_name='test_exchange',
            use_publisher_confirms=True
        )
        
        # Mock channel confirm_delivery and basic_publish
        self.mock_channel.confirm_delivery.return_value = None
        self.mock_channel.basic_publish.return_value = True  # Confirmed
        
        # Publish a message
        result = producer.publish('test message')
        
        # Verify confirm_delivery was called
        self.mock_channel.confirm_delivery.assert_called_once()
        
        # Check the result
        self.assertTrue(result)
        
    def test_publish_handles_confirm_failure(self):
        """Test that publish handles confirmation failures."""
        # Create a producer with confirms enabled
        producer = BaseProducer(
            exchange_name='test_exchange',
            use_publisher_confirms=True
        )
        
        # Mock channel confirm_delivery and basic_publish
        self.mock_channel.confirm_delivery.return_value = None
        self.mock_channel.basic_publish.return_value = False  # Not confirmed
        
        # Publish a message with only 1 retry
        result = producer.publish('test message', retry_count=1)
        
        # Check the result
        self.assertFalse(result)
        
    def test_publish_handles_unroutable_error(self):
        """Test that publish handles UnroutableError exceptions."""
        # Mock basic_publish to raise UnroutableError
        self.mock_channel.basic_publish.side_effect = pika.exceptions.UnroutableError("Test error")
        
        # Publish a message
        result = self.producer.publish('test message')
        
        # Check the result
        self.assertFalse(result)
        
    def test_publish_handles_general_exception(self):
        """Test that publish handles general exceptions."""
        # Mock basic_publish to raise an exception
        self.mock_channel.basic_publish.side_effect = Exception("Test error")
        
        # Publish a message with only 1 retry
        result = self.producer.publish('test message', retry_count=1)
        
        # Check the result
        self.assertFalse(result)
        
    def test_close(self):
        """Test closing the producer."""
        # Set up mock channel
        self.producer._channel = self.mock_channel
        
        # Close the producer
        self.producer.close()
        
        # Verify channel was closed
        self.mock_channel.close.assert_called_once()
        self.assertIsNone(self.producer._channel)
        
class TestOrderProducer(unittest.TestCase):
    """Tests for the OrderProducer class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock channel
        self.mock_channel = MagicMock()
        
        # Patch the get_channel function to return our mock
        self.channel_patcher = patch('rabbitmq_project.producers.base_producer.get_channel')
        self.mock_get_channel = self.channel_patcher.start()
        self.mock_get_channel.return_value = self.mock_channel
        
        # Create a test order producer
        self.order_producer = OrderProducer()
        
        # Replace publish method with mock
        self.order_producer.publish = MagicMock(return_value=True)
        
    def tearDown(self):
        """Clean up after each test."""
        self.channel_patcher.stop()
        
    def test_init(self):
        """Test initializing an order producer."""
        self.assertEqual(self.order_producer.exchange_name, 'orders')
        self.assertEqual(self.order_producer.exchange_type, 'topic')
        self.assertEqual(self.order_producer.routing_key, 'order')
        self.assertTrue(self.order_producer.use_publisher_confirms)
        
    def test_publish_order_created(self):
        """Test publishing an order created event."""
        order_data = {
            'id': 'test-order-123',
            'customer_id': 'customer-456',
            'customer_type': 'premium',
            'items': [
                {'product_id': 'product1', 'quantity': 2},
                {'product_id': 'product2', 'quantity': 1}
            ]
        }
        
        # Publish order created event
        result = self.order_producer.publish_order_created(order_data)
        
        # Check the result
        self.assertTrue(result)
        
        # Verify publish was called with correct arguments
        self.order_producer.publish.assert_called_once()
        args, kwargs = self.order_producer.publish.call_args
        
        # Check payload
        self.assertEqual(kwargs['message']['event_type'], 'created')
        self.assertEqual(kwargs['message']['data'], order_data)
        
        # Check routing key
        self.assertEqual(kwargs['routing_key'], 'order.created.premium')
        
        # Check headers
        self.assertEqual(kwargs['headers']['event'], 'created')
        
    def test_publish_order_created_generates_id(self):
        """Test that publish_order_created generates an ID if not provided."""
        order_data = {
            'customer_id': 'customer-456',
            'items': [{'product_id': 'product1', 'quantity': 1}]
        }
        
        # Publish order created event
        self.order_producer.publish_order_created(order_data)
        
        # Verify publish was called
        self.order_producer.publish.assert_called_once()
        args, kwargs = self.order_producer.publish.call_args
        
        # Check ID was generated
        self.assertTrue('id' in kwargs['message']['data'])
        
    def test_publish_order_updated(self):
        """Test publishing an order updated event."""
        order_id = 'test-order-123'
        updates = {'status': 'processing', 'updated_at': '2023-01-01T12:00:00'}
        
        # Publish order updated event
        result = self.order_producer.publish_order_updated(order_id, updates)
        
        # Check the result
        self.assertTrue(result)
        
        # Verify publish was called with correct arguments
        self.order_producer.publish.assert_called_once()
        args, kwargs = self.order_producer.publish.call_args
        
        # Check payload
        self.assertEqual(kwargs['message']['event_type'], 'updated')
        self.assertEqual(kwargs['message']['order_id'], order_id)
        self.assertEqual(kwargs['message']['updates'], updates)
        
        # Check routing key
        self.assertEqual(kwargs['routing_key'], 'order.updated')
        
        # Check headers
        self.assertEqual(kwargs['headers']['event'], 'updated')
        self.assertEqual(kwargs['headers']['order_id'], order_id)
        
    def test_publish_order_cancelled(self):
        """Test publishing an order cancelled event."""
        order_id = 'test-order-123'
        reason = 'out of stock'
        
        # Publish order cancelled event
        result = self.order_producer.publish_order_cancelled(order_id, reason)
        
        # Check the result
        self.assertTrue(result)
        
        # Verify publish was called with correct arguments
        self.order_producer.publish.assert_called_once()
        args, kwargs = self.order_producer.publish.call_args
        
        # Check payload
        self.assertEqual(kwargs['message']['event_type'], 'cancelled')
        self.assertEqual(kwargs['message']['order_id'], order_id)
        self.assertEqual(kwargs['message']['reason'], reason)
        
        # Check routing key
        self.assertEqual(kwargs['routing_key'], 'order.cancelled')
        
    def test_broadcast_order_notification(self):
        """Test broadcasting an order notification."""
        notification = {'message': 'Important system notification', 'level': 'info'}
        
        # Mock exchange_declare
        self.mock_channel.exchange_declare.return_value = None
        
        # Broadcast notification
        result = self.order_producer.broadcast_order_notification(notification)
        
        # Check the result
        self.assertTrue(result)
        
        # Verify exchange_declare was called for fanout exchange
        self.mock_channel.exchange_declare.assert_called_once_with(
            exchange='orders.notifications',
            exchange_type='fanout',
            durable=True
        )
        
        # Verify publish was called with correct arguments
        self.order_producer.publish.assert_called_once()
        args, kwargs = self.order_producer.publish.call_args
        
        # Check message
        self.assertEqual(kwargs['message'], notification)
        
        # Check exchange and routing key
        self.assertEqual(kwargs['exchange_name'], 'orders.notifications')
        self.assertEqual(kwargs['routing_key'], '')
        
if __name__ == '__main__':
    unittest.main()