# Distributed Messaging Queue System

A distributed messaging queue system using RabbitMQ as a message broker (exchanges only) with custom queuing and delivery. Supports ordered queuing/delivery, publisher-subscriber and work queue models, persistence, acknowledgment/retry, load balancing, clustering, and exchange types (direct, topic, fanout, headers).

## Setup

1. Install Docker and Python.
2. Run RabbitMQ cluster: `docker-compose up -d`.
3. Install dependencies: `pip install -r requirements.txt`.
4. Start queue manager: `./scripts/start_queue_manager.sh`.
5. Start delivery system: `./scripts/start_delivery.sh`.
6. Run consumers: `./scripts/start_consumers.sh`.
7. Run tests: `pytest tests/`.

## Structure

- `src/`: Producers, queue manager, delivery, consumers, retry logic.
- `tests/`: Unit and integration tests.
- `docker/`: RabbitMQ cluster setup.
- `scripts/`: Automation scripts.
- `docs/`: Documentation.

See `architecture.md` for system design.
