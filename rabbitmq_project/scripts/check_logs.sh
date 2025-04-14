#!/bin/bash

# A convenience script to check RabbitMQ logs from containers

NODE=${1:-rabbitmq1}

function usage() {
  echo "Usage: $0 [node_name] [options]"
  echo ""
  echo "Options:"
  echo "  -t, --tail     Tail the logs continuously"
  echo "  -l, --lines N  Show last N lines (default: 100)"
  echo "  -h, --help     Show this help"
  echo ""
  echo "Examples:"
  echo "  $0               # Show logs for rabbitmq1"
  echo "  $0 rabbitmq2     # Show logs for rabbitmq2"
  echo "  $0 rabbitmq3 -t  # Tail logs for rabbitmq3"
  echo ""
}

TAIL=false
LINES=100

# Parse options
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -t|--tail)
      TAIL=true
      shift
      ;;
    -l|--lines)
      LINES="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    rabbitmq*)
      NODE="$1"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if ! docker ps | grep -q $NODE; then
  echo "Error: Container $NODE is not running"
  exit 1
fi

LOG_FILE="/var/log/rabbitmq/rabbit@${NODE}.log"

echo "Checking logs for $NODE"
if [ "$TAIL" = true ]; then
  echo "Tailing logs from $LOG_FILE"
  docker exec -it $NODE bash -c "tail -f $LOG_FILE"
else
  echo "Showing last $LINES lines from $LOG_FILE"
  docker exec -it $NODE bash -c "tail -n $LINES $LOG_FILE"
fi