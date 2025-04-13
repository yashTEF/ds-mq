"""
Unit tests for the retry handler component.
Tests the functionality of the retry mechanism for failed message processing.
"""
import unittest
from unittest.mock import MagicMock, patch, call
import json
import time
import pika
from ..core.retry_handler import RetryHandler, retry_handler

class TestRetryHandler(unittest.TestCase):
    """Tests for the RetryHandler class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock channel
        self.mock_channel = MagicMock()
        
        # Create basic message props and delivery info
        self.mock_properties = MagicMock()
        self.mock_properties.headers = {}
        self.mock_properties.content_type = 'application/json'
        
        self.mock_delivery = MagicMock()
        self.mock_delivery.delivery_tag = 123
        self.mock_delivery.routing_key = 'test_key'
        
        # Create a test message body
        self.message_body = json.dumps({'test': 'data'}).encode('utf-8')
        
        # Reset the retry handler singleton
        retry_handler._instance = None
        
    def tearDown(self):
        """Clean up after each test."""
        retry_handler._instance = None
        
    def test_singleton_pattern(self):
        """Test that RetryHandler is a singleton."""
        handler1 = RetryHandler()
        handler2 = RetryHandler()
        
        self.assertIs(handler1, handler2)
        self.assertIs(handler1, retry_handler)
        
    def test_init(self):
        """Test initializing a retry handler."""
        handler = RetryHandler()
        
        # Check default values
        self.assertEqual(handler.max_retries, 3)
        self.assertEqual(handler.initial_delay, 1.0)
        self.assertEqual(handler.backoff_factor, 2.0)
        self.assertEqual(handler.dlx_name, 'dead-letter-exchange')
        self.assertTrue(handler.dlx_enabled)
        
    @patch('rabbitmq_project.core.retry_handler.get_channel')
    def test_setup_dlx(self, mock_get_channel):
        """Test setting up the dead letter exchange."""
        mock_channel = MagicMock()
        mock_get_channel.return_value = mock_channel
        
        handler = RetryHandler()
        handler._setup_dlx()
        
        # Verify DLX was declared
        mock_channel.exchange_declare.assert_called_once_with(
            exchange='dead-letter-exchange',
            exchange_type='direct',
            durable=True
        )
        
        # Verify DLQ was declared
        mock_channel.queue_declare.assert_called_once_with(
            queue='dead-letter-queue',
            durable=True,
            arguments={
                'x-message-ttl': 1000 * 60 * 60 * 24 * 7  # 7 days
            }
        )
        
        # Verify queue was bound to exchange
        mock_channel.queue_bind.assert_called_once_with(
            queue='dead-letter-queue',
            exchange='dead-letter-exchange',
            routing_key='#'
        )
        
    def test_calculate_delay_exponential_backoff(self):
        """Test calculating delay with exponential backoff."""
        handler = RetryHandler(initial_delay=1.0, backoff_factor=2.0)
        
        self.assertEqual(handler._calculate_delay(1), 1.0)
        self.assertEqual(handler._calculate_delay(2), 2.0)
        self.assertEqual(handler._calculate_delay(3), 4.0)
        
    def test_process_retry_first_attempt(self):
        """Test processing a retry for the first time."""
        handler = RetryHandler()
        
        # Mock republish method
        handler._republish_with_retry_header = MagicMock()
        
        # Process retry with no existing count
        handler.process_retry(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            queue_name='test_queue'
        )
        
        # Check that republish was called with retry count 1
        handler._republish_with_retry_header.assert_called_once()
        args = handler._republish_with_retry_header.call_args[0]
        self.assertEqual(args[0], self.mock_channel)
        self.assertEqual(args[1], 'test_queue')
        self.assertEqual(args[2], self.mock_properties)
        self.assertEqual(args[3], self.message_body)
        self.assertEqual(args[4], 1)  # retry count
        
        # Check rejection
        self.mock_channel.basic_reject.assert_called_once_with(
            delivery_tag=self.mock_delivery.delivery_tag,
            requeue=False
        )
        
    def test_process_retry_with_existing_count(self):
        """Test processing a retry with an existing count in headers."""
        handler = RetryHandler()
        
        # Set up properties with existing retry count
        self.mock_properties.headers = {'x-retry-count': 2}
        
        # Mock republish method
        handler._republish_with_retry_header = MagicMock()
        
        # Process retry
        handler.process_retry(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            queue_name='test_queue'
        )
        
        # Check that republish was called with retry count 3
        handler._republish_with_retry_header.assert_called_once()
        args = handler._republish_with_retry_header.call_args[0]
        self.assertEqual(args[4], 3)  # retry count
        
    def test_process_retry_exceeds_max_retries(self):
        """Test processing a retry that exceeds max retries."""
        handler = RetryHandler(max_retries=3)
        
        # Set up properties with existing retry count at max
        self.mock_properties.headers = {'x-retry-count': 3}
        
        # Mock send to DLX method
        handler._send_to_dead_letter_exchange = MagicMock()
        
        # Process retry
        handler.process_retry(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            queue_name='test_queue'
        )
        
        # Check that send to DLX was called
        handler._send_to_dead_letter_exchange.assert_called_once_with(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            'test_queue'
        )
        
        # Check rejection
        self.mock_channel.basic_reject.assert_called_once_with(
            delivery_tag=self.mock_delivery.delivery_tag,
            requeue=False
        )
        
    def test_process_retry_with_exception(self):
        """Test processing a retry with an exception."""
        handler = RetryHandler()
        
        # Mock republish method
        handler._republish_with_retry_header = MagicMock()
        
        # Create an exception
        exception = ValueError("Test exception")
        
        # Process retry with exception
        handler.process_retry(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            queue_name='test_queue',
            exception=exception
        )
        
        # Check that republish was called with error details
        handler._republish_with_retry_header.assert_called_once()
        kwargs = handler._republish_with_retry_header.call_args[1]
        
        # Verify error details were added
        self.assertIn('error_details', kwargs)
        self.assertIn('ValueError', kwargs['error_details'])
        self.assertIn('Test exception', kwargs['error_details'])
        
    def test_republish_with_retry_header(self):
        """Test republishing a message with retry headers."""
        handler = RetryHandler()
        
        # Process republish
        handler._republish_with_retry_header(
            self.mock_channel,
            'test_queue',
            self.mock_properties,
            self.message_body,
            2
        )
        
        # Check that basic_publish was called with correct parameters
        self.mock_channel.basic_publish.assert_called_once()
        args, kwargs = self.mock_channel.basic_publish.call_args
        
        # Check exchange and routing key
        self.assertEqual(kwargs['exchange'], '')
        self.assertEqual(kwargs['routing_key'], 'test_queue')
        
        # Check retry header was added
        properties = kwargs['properties']
        self.assertEqual(properties.headers['x-retry-count'], 2)
        
    @patch('time.sleep')
    def test_republish_with_delay(self, mock_sleep):
        """Test republishing a message with delay."""
        handler = RetryHandler(initial_delay=1.0, backoff_factor=2.0)
        
        # Process republish with delay
        handler._republish_with_retry_header(
            self.mock_channel,
            'test_queue',
            self.mock_properties,
            self.message_body,
            retry_count=3
        )
        
        # Check that sleep was called with correct delay (2^(3-1) = 4 seconds)
        mock_sleep.assert_called_once_with(4.0)
        
    def test_send_to_dead_letter_exchange(self):
        """Test sending a message to the dead letter exchange."""
        handler = RetryHandler()
        
        # Mock setup DLX to ensure it's called
        handler._setup_dlx = MagicMock()
        
        # Send message to DLX
        handler._send_to_dead_letter_exchange(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            'test_queue'
        )
        
        # Check that _setup_dlx was called
        handler._setup_dlx.assert_called_once()
        
        # Check that basic_publish was called with correct parameters
        self.mock_channel.basic_publish.assert_called_once()
        args, kwargs = self.mock_channel.basic_publish.call_args
        
        # Check exchange and routing key
        self.assertEqual(kwargs['exchange'], 'dead-letter-exchange')
        self.assertEqual(kwargs['routing_key'], 'test_queue')
        
        # Check additional headers were added
        properties = kwargs['properties']
        self.assertIn('x-original-queue', properties.headers)
        self.assertEqual(properties.headers['x-original-queue'], 'test_queue')
        self.assertIn('x-death-time', properties.headers)
        
    def test_disabled_dlx(self):
        """Test behavior when DLX is disabled."""
        handler = RetryHandler(dlx_enabled=False)
        
        # Set up properties with existing retry count at max
        self.mock_properties.headers = {'x-retry-count': 3}
        
        # Mock send to DLX method to ensure it's not called
        handler._send_to_dead_letter_exchange = MagicMock()
        
        # Process retry
        handler.process_retry(
            self.mock_channel,
            self.mock_delivery,
            self.mock_properties,
            self.message_body,
            queue_name='test_queue'
        )
        
        # Check that send to DLX was not called
        handler._send_to_dead_letter_exchange.assert_not_called()
        
        # Check that message was rejected with requeue=False
        self.mock_channel.basic_reject.assert_called_once_with(
            delivery_tag=self.mock_delivery.delivery_tag,
            requeue=False
        )
        
if __name__ == '__main__':
    unittest.main()