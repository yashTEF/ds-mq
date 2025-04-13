#!/bin/bash

# Script to create the distributed-mq project structure
# Creates directories and placeholder files for a RabbitMQ-based messaging system

# Define project root
PROJECT_NAME="distributed-mq"

# Create project root directory
echo "Creating project directory: $PROJECT_NAME"
mkdir -p "$PROJECT_NAME"
cd "$PROJECT_NAME" || exit 1

# Create source directories and files
echo "Creating src/ directories and files..."
mkdir -p src/producers
mkdir -p src/queue_manager
mkdir -p src/delivery
mkdir -p src/consumers
mkdir -p src/common
mkdir -p src/retry

# Create producer scripts
touch src/producers/direct_producer.py
touch src/producers/topic_producer.py
touch src/producers/fanout_producer.py
touch src/producers/headers_producer.py

# Create queue manager scripts
touch src/queue_manager/queue_manager.py
touch src/queue_manager/storage.py

# Create delivery scripts
touch src/delivery/delivery_system.py
touch src/delivery/load_balancer.py

# Create consumer scripts
touch src/consumers/pubsub_consumer.py
touch src/consumers/work_queue_consumer.py

# Create common scripts
touch src/common/config.py
touch src/common/utils.py

# Create retry script
touch src/retry/retry_handler.py

# Create tests directory and files
echo "Creating tests/ directory and files..."
mkdir -p tests
touch tests/test_direct.py
touch tests/test_topic.py
touch tests/test_fanout.py
touch tests/test_headers.py
touch tests/test_persistence.py
touch tests/test_ack_retry.py
touch tests/test_load_balancing.py
touch tests/test_patterns.py

# Create docker directory and files
echo "Creating docker/ directory and files..."
mkdir -p docker/rabbitmq-node1
mkdir -p docker/rabbitmq-node2
touch docker/docker-compose.yml

# Create scripts directory and files
echo "Creating scripts/ directory and files..."
mkdir -p scripts
touch scripts/setup_cluster.sh
touch scripts/start_queue_manager.sh
touch scripts/start_delivery.sh
touch scripts/start_consumers.sh

# Create docs directory and files
echo "Creating docs/ directory and files..."
mkdir -p docs
touch docs/README.md
touch docs/architecture.md

# Create requirements.txt with basic dependencies
echo "Creating requirements.txt..."
cat <<EOL > requirements.txt
pika==1.3.2
pytest==7.4.0
EOL

# Create .gitignore
echo "Creating .gitignore..."
cat <<EOL > .gitignore
# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
*.egg-info/

# SQLite database
queue.db

# Logs
*.log

# Docker
docker-compose.override.yml

# IDE files
.idea/
.vscode/

# OS files
.DS_Store
Thumbs.db
EOL

# Initialize README.md with basic content
echo "Creating README.md..."
cat <<EOL > docs/README.md
# Distributed Messaging Queue System

A distributed messaging queue system using RabbitMQ as a message broker (exchanges only) with custom queuing and delivery. Supports ordered queuing/delivery, publisher-subscriber and work queue models, persistence, acknowledgment/retry, load balancing, clustering, and exchange types (direct, topic, fanout, headers).

## Setup

1. Install Docker and Python.
2. Run RabbitMQ cluster: \`docker-compose up -d\`.
3. Install dependencies: \`pip install -r requirements.txt\`.
4. Start queue manager: \`./scripts/start_queue_manager.sh\`.
5. Start delivery system: \`./scripts/start_delivery.sh\`.
6. Run consumers: \`./scripts/start_consumers.sh\`.
7. Run tests: \`pytest tests/\`.

## Structure

- \`src/\`: Producers, queue manager, delivery, consumers, retry logic.
- \`tests/\`: Unit and integration tests.
- \`docker/\`: RabbitMQ cluster setup.
- \`scripts/\`: Automation scripts.
- \`docs/\`: Documentation.

See \`architecture.md\` for system design.
EOL

# Initialize architecture.md with placeholder
echo "Creating architecture.md..."
cat <<EOL > docs/architecture.md
# System Architecture

This document describes the architecture of the distributed messaging queue system.

## Overview

- **Producers**: Send messages to RabbitMQ exchanges.
- **RabbitMQ**: Routes messages via exchanges (direct, topic, fanout, headers).
- **Queue Manager**: Stores messages in SQLite (custom queuing).
- **Delivery System**: Distributes messages to consumers (custom delivery).
- **Consumers**: Process messages for pub-sub or work queue patterns.

## Components

(TBD: Add detailed architecture, diagrams, and trade-offs after implementation.)
EOL

# Initialize docker-compose.yml with basic RabbitMQ cluster
echo "Creating docker-compose.yml..."
cat <<EOL > docker/docker-compose.yml
version: '3'
services:
  rabbit1:
    image: rabbitmq:3-management
    hostname: rabbit1
    environment:
      - RABBITMQ_ERLANG_COOKIE='secretcookie'
    ports:
      - "5672:5672"
      - "15672:15672"
  rabbit2:
    image: rabbitmq:3-management
    hostname: rabbit2
    environment:
      - RABBITMQ_ERLANG_COOKIE='secretcookie'
    ports:
      - "5673:5672"
      - "15673:15672"
EOL

# Initialize config.py with basic settings
echo "Creating config.py..."
cat <<EOL > src/common/config.py
# Configuration for RabbitMQ and SQLite
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
SQLITE_DB = 'queue.db'
EOL

# Initialize setup_cluster.sh with placeholder
echo "Creating setup_cluster.sh..."
cat <<EOL > scripts/setup_cluster.sh
#!/bin/bash
# Script to configure RabbitMQ cluster
echo "Configuring RabbitMQ cluster..."
# TODO: Add clustering commands (e.g., join_cluster)
docker exec distributed-mq-rabbit1-1 rabbitmqctl cluster_status
EOL
chmod +x scripts/setup_cluster.sh

# Initialize other scripts as placeholders
echo "Creating placeholder scripts..."
for script in start_queue_manager.sh start_delivery.sh start_consumers.sh; do
  cat <<EOL > scripts/$script
#!/bin/bash
# Placeholder for $script
echo "Starting $script..."
# TODO: Add implementation
EOL
  chmod +x scripts/$script
done

# Set permissions for Python files
echo "Setting permissions..."
find src -type f -name "*.py" -exec chmod 644 {} \;
find tests -type f -name "*.py" -exec chmod 644 {} \;

# Set execute permissions for scripts
find scripts -type f -name "*.sh" -exec chmod +x {} \;

