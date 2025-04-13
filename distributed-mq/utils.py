import pika
import os
import logging
from prometheus_client import start_http_server
from threading import Thread

def get_connection():
    """Establish a connection to RabbitMQ using environment variables or defaults."""
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    port = int(os.environ.get('RABBITMQ_PORT', '5672'))
    user = os.environ.get('RABBITMQ_USER', 'guest')
    password = os.environ.get('RABBITMQ_PASSWORD', 'guest')
    credentials = pika.PlainCredentials(user, password)
    return pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', credentials))

def declare_queue(channel, queue_name, durable=False):
    """Declare a queue with optional durability."""
    channel.queue_declare(queue=queue_name, durable=durable)

def declare_exchange(channel, exchange_name, exchange_type):
    """Declare an exchange with the specified type."""
    channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

def bind_queue(channel, queue_name, exchange_name, binding_key=None, arguments=None):
    """Bind a queue to an exchange with a binding key or arguments."""
    channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=binding_key, arguments=arguments)

def parse_headers(headers_str):
    """Parse a string of headers (e.g., 'key1=value1,key2=value2') into a dictionary."""
    if not headers_str:
        return {}
    headers = {}
    try:
        for pair in headers_str.split(','):
            key, value = pair.split('=')
            headers[key.strip()] = value.strip()
        return headers
    except ValueError as e:
        raise ValueError(f"Invalid headers format: {headers_str}. Use 'key1=value1,key2=value2'.") from e

def configure_logging(log_file):
    """Configure logging to file and console."""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def start_metrics_server(port=8000):
    """Start Prometheus metrics server in a separate thread."""
    def run_server():
        start_http_server(port)
        logging.getLogger('Metrics').info(f"Metrics server started on port {port}")
    thread = Thread(target=run_server)
    thread.daemon = True
    thread.start()