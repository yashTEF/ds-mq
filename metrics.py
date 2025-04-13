import argparse
import logging
from utils import start_metrics_server, configure_logging

def main():
    parser = argparse.ArgumentParser(description="Start Prometheus metrics server.")
    parser.add_argument('--port', type=int, default=8000, help='Port for metrics server')
    parser.add_argument('--name', default='metrics', help='Name for logging')

    args = parser.parse_args()

    configure_logging(f'logs/{args.name}.log')
    logger = logging.getLogger('Metrics')
    logger.info(f"Starting metrics server on port {args.port}")
    start_metrics_server(args.port)

    # Keep the script running
    try:
        while True:
            pass
    except KeyboardInterrupt:
        logger.info("Metrics server stopped")

if __name__ == '__main__':
    main()