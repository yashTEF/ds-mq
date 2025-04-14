import pika
import json
import time
import logging
import threading
import uuid
from datetime import datetime
from pika.exceptions import IncompatibleProtocolError, StreamLostError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Producer Test Class
class RabbitMQProducerTest:
    def __init__(self, host='10.1.37.37', port=5672, user='admin', password='admin123'):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(user, password)
        self.connection = None
        self.channel = None
        self.exchange_name = 'test_exchange302'
        self.queue_name = 'test_queue302'
        self.routing_key = 'test.message302'
    
    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host='/',
                    credentials=self.credentials,
                    heartbeat=30,
                    socket_timeout=5
                )
            )
            logger.info("✅ Producer connection established")
            self.channel = self.connection.channel()
            logger.info("✅ Producer channel created")
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )
            logger.info(f"✅ Exchange '{self.exchange_name}' declared")
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"✅ Queue '{self.queue_name}' declared")
            self.channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.routing_key
            )
            logger.info(f"✅ Queue bound to exchange with routing key '{self.routing_key}'")
            return True
        except Exception as e:
            logger.error(f"❌ Producer connection failed: {e}")
            return False

    def send_message(self, message_data):
        try:
            if not hasattr(self.channel, '_confirms_enabled'):
                self.channel.confirm_delivery()
                setattr(self.channel, '_confirms_enabled', True)
                logger.info("✅ Publisher confirms enabled")
            body = json.dumps(message_data).encode()
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    message_id=str(uuid.uuid4()),
                    timestamp=int(time.time())
                ),
                mandatory=True
            )
            if self.connection.is_open:
                tx_buffers = getattr(self.connection._impl, '_tx_buffers', None)
                if tx_buffers is not None and len(tx_buffers) > 0:
                    self.connection.process_data_events(time_limit=0)
            logger.info(f"✅ Message sent: {message_data}")
            return True
        except Exception as e:
            logger.error(f"❌ Error sending message: {e}")
            return False

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info("✅ Producer connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing producer connection: {e}")

    def run_test(self):
        if self.connect():
            num_messages = 5
            for i in range(1, num_messages + 1):
                msg = {
                    'id': str(uuid.uuid4()),
                    'sequence': i,
                    'content': f'Test message {i} of {num_messages}',
                    'timestamp': datetime.now().isoformat()
                }
                self.send_message(msg)
                time.sleep(0.5)
            self.close()
        else:
            logger.error("Producer test aborted due to connection failure")


# Consumer Test Class
class RabbitMQConsumerTest:
    def __init__(self, host='localhost', port=5672, user='admin', password='admin123'):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(user, password)
        self.consumer_connection = None
        self.consumer_channel = None
        self.exchange_name = 'test_exchange302'
        self.queue_name = 'test_queue302'
        self.routing_key = 'test.message302'
        self.consumer_tag = None
        self.messages_received = 0
        self.consumer_thread = None
        self.stop_consuming = threading.Event()
    
    def connect(self):
        try:
            self.consumer_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host='/',
                    credentials=self.credentials,
                    heartbeat=30,
                    socket_timeout=5
                )
            )
            logger.info("✅ Consumer connection established")
            self.consumer_channel = self.consumer_connection.channel()
            logger.info("✅ Consumer channel created")
            self.consumer_channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )
            self.consumer_channel.queue_declare(queue=self.queue_name, durable=True)
            self.consumer_channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=self.routing_key
            )
            return True
        except Exception as e:
            logger.error(f"❌ Consumer connection failed: {e}")
            return False

    def message_callback(self, ch, method, properties, body):
        try:
            data = json.loads(body)
            self.messages_received += 1
            logger.info(f"✅ Message #{self.messages_received} received: {data}")
            time.sleep(0.2)  # Simulate processing delay
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"✅ Message #{self.messages_received} acknowledged")
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _consume_thread(self):
        try:
            while not self.stop_consuming.is_set():
                if not self.consumer_connection.is_open:
                    logger.info("Consumer connection closed, exiting thread")
                    break
                try:
                    self.consumer_connection.process_data_events(time_limit=1)
                except (StreamLostError, IndexError) as e:
                    logger.info(f"{e.__class__.__name__} in consumer loop: {e}. Exiting thread.")
                    break
                except Exception as ex:
                    logger.error(f"❌ Error in consumer loop: {ex}")
                    time.sleep(1)
        except Exception as e:
            logger.error(f"❌ Consumer thread error: {e}")
        finally:
            logger.info("✅ Consumer thread stopped")

    def start_consuming(self):
        if not self.consumer_connection or self.consumer_connection.is_closed:
            if not self.connect():
                return False
        self.consumer_channel.basic_qos(prefetch_count=1)
        self.consumer_tag = self.consumer_channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.message_callback
        )
        logger.info(f"✅ Started consuming from '{self.queue_name}'")
        self.stop_consuming.clear()
        self.consumer_thread = threading.Thread(target=self._consume_thread)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        return True

    def stop(self):
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.stop_consuming.set()
            if self.consumer_channel and self.consumer_tag:
                try:
                    self.consumer_channel.basic_cancel(self.consumer_tag)
                except Exception:
                    pass
            self.consumer_thread.join(timeout=2)
            logger.info("✅ Consumer stopped")

    def close(self):
        try:
            if self.consumer_connection and self.consumer_connection.is_open:
                self.consumer_connection.close()
                logger.info("✅ Consumer connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing consumer connection: {e}")

    def run_test(self):
        if self.connect():
            self.start_consuming()
            time.sleep(5)  # Allow time for messages to be received
            self.stop()
            self.close()
        else:
            logger.error("Consumer test aborted due to connection failure")


if __name__ == "__main__":
    logger.info("🚀 Starting Producer Test")
    producer_test = RabbitMQProducerTest()
    producer_test.run_test()

    logger.info("🚀 Starting Consumer Test")
    consumer_test = RabbitMQConsumerTest()
    consumer_test.run_test()