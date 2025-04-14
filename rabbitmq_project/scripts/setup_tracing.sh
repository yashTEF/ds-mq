#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p ../logs

# Wait for RabbitMQ to be fully up and running
echo "Waiting for RabbitMQ to start..."
until docker exec rabbitmq1 rabbitmqctl status >/dev/null 2>&1; do
  sleep 2
done
echo "RabbitMQ is up!"

# Enable tracing in RabbitMQ
echo "Enabling RabbitMQ tracing..."
docker exec rabbitmq1 rabbitmqctl trace_on
docker exec rabbitmq1 bash -c "rabbitmqctl set_user_tags admin administrator monitoring tracing"

# Create a trace for all messages
echo "Setting up trace for all messages..."
curl -u admin:admin123 -X PUT http://localhost:15672/api/traces/%2f/all-messages \
  -H "Content-Type: application/json" \
  -d '{"pattern":"#", "name":"all-messages", "format":"text", "tracer_connection_username":"admin", "tracer_connection_password":"admin123", "payload_bytes":10000}'

echo "Message tracing enabled successfully!"
echo "View traces in the RabbitMQ management UI: http://localhost:15672/#/traces"
echo ""
echo "To view logs from the container, run:"
echo "docker exec rabbitmq1 cat /var/log/rabbitmq/rabbit@rabbitmq1.log"
echo ""
echo "To continuously monitor logs:"
echo "docker exec rabbitmq1 tail -f /var/log/rabbitmq/rabbit@rabbitmq1.log"