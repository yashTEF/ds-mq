# Distributed RabbitMQ Messaging System

A robust, scalable distributed messaging system built with RabbitMQ for high-throughput message processing.

## Architecture

This project implements a distributed message queue system using RabbitMQ with the following features:

- **High Availability**: RabbitMQ cluster with multiple nodes
- **Load Balancing**: HAProxy for client connections
- **Message Patterns**: Supports multiple messaging patterns (direct, topic, fanout, headers)
- **Resilience**: Automatic message retry and dead-letter handling
- **Monitoring**: Prometheus and Grafana for real-time metrics

## Components

### Core Infrastructure
- **RabbitMQ Cluster**: 3-node cluster for redundancy and high availability
- **HAProxy**: Load balancer for client connections
- **Prometheus & Grafana**: Monitoring and visualization

### Application Components
- **Connection Management**: Handles connections to RabbitMQ
- **Channel Management**: Manages channel lifecycle
- **Exchange Management**: Creates and configures exchanges
- **Queue Management**: Creates and configures queues
- **Binding Management**: Sets up routing between exchanges and queues
- **Retry Handler**: Implements retry logic for failed messages
- **Message Router**: Routes messages based on headers and content

### Producers and Consumers
- **BaseProducer**: Core producer functionality
- **OrderProducer**: Specialized producer for order processing
- **BaseConsumer**: Core consumer functionality
- **InventoryConsumer**: Specialized consumer for inventory management

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/rabbitmq-project.git
cd rabbitmq-project
```

2. Start the RabbitMQ cluster:
```bash
docker-compose up -d
```

3. Access services:
   - RabbitMQ Management: http://localhost:15672 (admin/admin123)
   - HAProxy Stats: http://localhost:1936 (admin/admin123)
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin123)

### Running the Services

Start the producer and consumer services:

```bash
docker-compose up -d order_producer inventory_consumer
```

## Message Flow

1. **Order Creation**:
   - Order producer publishes to `orders` exchange
   - Messages are routed based on order type
   - Inventory consumer processes incoming orders

2. **Order Updates**:
   - Changes to orders are published with routing key updates
   - Interested consumers receive relevant updates

3. **Notifications**:
   - System-wide notifications use fanout exchange

## Configuration

Configuration is managed through environment variables:

- `RABBITMQ_HOST`: RabbitMQ host (default: haproxy)
- `RABBITMQ_PORT`: RabbitMQ port (default: 5670)
- `RABBITMQ_USER`: RabbitMQ username (default: admin)
- `RABBITMQ_PASSWORD`: RabbitMQ password (default: admin123)
- `PRODUCER_TYPE`: Type of producer (default: order)
- `CONSUMER_TYPE`: Type of consumer (default: inventory)
- `CONSUMER_INSTANCES`: Number of consumer instances (default: 3)
- `PREFETCH_COUNT`: Consumer prefetch count (default: 10)

## Testing

Run tests with pytest:

```bash
pytest
```

## Monitoring

The system exposes various metrics:

- **RabbitMQ Node Metrics**: Queue depth, message rates
- **Application Metrics**: Processing time, failure rates
- **System Metrics**: CPU, memory usage

## License

This project is licensed under the MIT License - see the LICENSE file for details.