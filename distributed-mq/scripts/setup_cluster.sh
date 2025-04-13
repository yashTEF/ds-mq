#!/bin/bash
# Script to configure RabbitMQ cluster
echo "Configuring RabbitMQ cluster..."
# TODO: Add clustering commands (e.g., join_cluster)
docker exec distributed-mq-rabbit1-1 rabbitmqctl cluster_status
