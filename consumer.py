import argparse
import pika
import time
import random
import logging
from prometheus_client import Counter
from utils import get_connection, declare_queue, declare_exchange, bind_queue, parse_headers, configure_logging

# Prometheus metric
messages_processed = Counter('messages_processed_total', 'Total messages processed by consumer', ['queue'])

def callback(ch, method, properties, body):
    """Callback function to process received messages."""
    message = body.decode()
    logger = logging.getLogger('Consumer')
    logger.info(f"Received: {message}")
    # Simulate processing time
    time.sleep(1)
    # Simulate random failure (20% chance) to demonstrate retry
    if random.random() < 0.2 and args.ack_mode == 'manual':
        logger.warning(f"Simulating failure for message: {message}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    elif args.ack_mode == 'manual':
        logger.info(f"Successfully processed: {message}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        messages_processed.labels(queue=method.routing_key).inc()

def main():
    # Configure logging
    configure_logging('logs/consumer.log')
    global logger
    logger = logging.getLogger('Consumer')

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Consume messages from RabbitMQ.")
    parser.add_argument('--queue', help='Queue name to consume from')
    parser.add_argument('--exchange', help='Exchange name to bind to')
    parser.add_argument('--exchange_type', choices=['direct', 'topic', 'fanout', 'headers'],
                        help='Type of exchange')
    parser.add_argument('--binding_key', default='', help='Binding key for direct/topic exchanges')
    parser.add_argument('--x_match', choices=['all', 'any'],
                        help='x-match type for headers exchange (all or any)')
    parser.add_argument('--headers', help='Headers for headers exchange binding (e.g., "key1=value1,key2=value2")')
    parser.add_argument('--ack_mode', choices=['auto', 'manual'], default='auto',
                        help='Acknowledgment mode')
    parser.add_argument('--prefetch', type=int, default=1, help='Prefetch count for load balancing')

    global args
    args = parser.parse_args()

    # Establish connection and channel
    try:
        connection = get_connection()
        channel = connection.channel()
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        print(f"Error: Connection failed: {e}")
        return

    # Set prefetch for load balancing
    try:
        channel.basic_qos(prefetch_count=args.prefetch)
    except Exception as e:
        logger.error(f"Failed to set prefetch: {e}")
        print(f"Error: {e}")
        return

    # Handle direct queue scenario
    if args.queue:
        try:
            declare_queue(channel, args.queue, durable=True)
            queue_name = args.queue
        except Exception as e:
            logger.error(f"Failed to declare queue '{args.queue}': {e}")
            print(f"Error: {e}")
            return

    # Handle exchange scenario
    elif args.exchange:
        if not args.exchange_type:
            logger.error("Exchange type required when exchange is specified")
            print("Error: --exchange_type is required when --exchange is specified.")
            return
        try:
            declare_exchange(channel, args.exchange, args.exchange_type)
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            if args.exchange_type == 'headers':
                if not args.x_match or not args.headers:
                    logger.error("Must specify x_match and headers for headers exchange")
                    print("Error: Must specify --x_match and --headers for headers exchange.")
                    return
                headers_dict = parse_headers(args.headers)
                arguments = {'x-match': args.x_match, **headers_dict}
                bind_queue(channel, queue_name, args.exchange, arguments=arguments)
            else:
                bind_queue(channel, queue_name, args.exchange, binding_key=args.binding_key)
        except Exception as e:
            logger.error(f"Failed to set up exchange '{args.exchange}': {e}")
            print(f"Error: {e}")
            return

    else:
        logger.error("Must specify either queue or exchange")
        print("Error: Must specify either --queue or --exchange.")
        return

    # Set up consumer
    try:
        auto_ack = (args.ack_mode == 'auto')
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)
        logger.info(f"Started consuming from {queue_name}")
        print(f"Started consuming from {queue_name}. Press CTRL+C to exit.")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
        channel.stop_consuming()
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        print(f"Error: {e}")
    finally:
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

if __name__ == '__main__':
    main()