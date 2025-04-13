# Distributed Messaging Queue System with RabbitMQ

This project implements a distributed messaging queue system using RabbitMQ, supporting basic queuing, Publisher-Subscriber, Work Queue models, message persistence, acknowledgment/retry mechanisms, load balancing, and all exchange types (direct, topic, fanout, headers).

## Prerequisites

- **RabbitMQ**: Installed and running (e.g., on `localhost:5672` with default `guest:guest` credentials).
- **Python**: Version 3.6+.
- **Pika**: Install with `pip install pika`.

## Files

- `producer.py`: Sends messages to queues or exchanges.
- `consumer.py`: Consumes messages from queues or exchanges.
- `utils.py`: Utility functions for RabbitMQ operations.

## Setup

1. **Install Dependencies**:
   ```bash
   pip install pika
   ```
