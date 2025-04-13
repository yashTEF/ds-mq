import argparse
import pika
import logging
from prometheus_client import Counter
from utils import get_connection, declare_queue, declare_exchange, parse_headers, configure_logging

# Prometheus metric
messages_sent = Counter('messages_sent_total', 'Total messages sent by producer', ['exchange', 'queue'])

def main():
    # Configure logging
    configure_logging('logs/producer.log')
    logger = logging.getLogger('Producer')

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Send messages to RabbitMQ.")
    parser.add_argument('--queue', help='Queue name for direct sending')
    parser.add_argument('--exchange', help='Exchange name')
    parser.add_argument('--exchange_type', choices=['direct', 'topic', 'fanout', 'headers'],
                        help='Type of exchange')
    parser.add_argument('--routing_key', default='', help='Routing key for the message')
    parser.add_argument('--message', required=True, help='Message body to send')
    parser.add_argument('--persistent', action='store_true', help='Make the message persistent')
    parser.add_argument('--headers', help='Headers for headers exchange (e.g., "key1=value1,key2=value2")')

    args = parser.parse_args()

    # Establish connection and channel
    try:
        connection = get_connection()
        channel = connection.channel()
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        print(f"Error: Connection failed: {e}")
        return

    # Handle direct queue scenario
    if args.queue:
        try:
            declare_queue(channel, args.queue, durable=True)
            properties = pika.BasicProperties(delivery_mode=2 if args.persistent else 1)
            channel.basic_publish(
                exchange='',
                routing_key=args.queue,
                body=args.message.encode(),
                properties=properties
            )
            logger.info(f"Sent to queue '{args.queue}': {args.message}")
            messages_sent.labels(exchange='', queue=args.queue).inc()
            print(f"Sent to queue '{args.queue}': {args.message}")
        except Exception as e:
            logger.error(f"Failed to send to queue '{args.queue}': {e}")
            print(f"Error: {e}")

    # Handle exchange scenario
    elif args.exchange:
        if not args.exchange_type:
            logger.error("Exchange type required when exchange is specified")
            print("Error: --exchange_type is required when --exchange is specified.")
            return
        try:
            declare_exchange(channel, args.exchange, args.exchange_type)
            properties = pika.BasicProperties(delivery_mode=2 if args.persistent else 1)
            if args.headers:
                headers_dict = parse_headers(args.headers)
                properties.headers = headers_dict
            channel.basic_publish(
                exchange=args.exchange,
                routing_key=args.routing_key,
                body=args.message.encode(),
                properties=properties
            )
            logger.info(f"Sent to exchange '{args.exchange}' (type: {args.exchange_type}): {args.message}")
            messages_sent.labels(exchange=args.exchange, queue='').inc()
            print(f"Sent to exchange '{args.exchange}' (type: {args.exchange_type}): {args.message}")
        except Exception as e:
            logger.error(f"Failed to send to exchange '{args.exchange}': {e}")
            print(f"Error: {e}")

    else:
        logger.error("Must specify either queue or exchange")
        print("Error: Must specify either --queue or --exchange.")
        return

    # Clean up
    try:
        connection.close()
    except Exception as e:
        logger.warning(f"Error closing connection: {e}")

if __name__ == '__main__':
    main()