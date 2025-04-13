"""
Monitoring service for RabbitMQ.
Collects metrics from RabbitMQ's Prometheus endpoint and exposes them for Grafana.
"""
import time
import threading
import requests
from prometheus_client import start_http_server, Gauge, Counter
from ..core.logger import get_logger
from ..config.config import config

# Module logger
logger = get_logger(__name__)

class MonitoringService:
    """
    Service to monitor RabbitMQ and expose metrics.
    Scrapes RabbitMQ's built-in Prometheus endpoint or HTTP API.
    """
    
    def __init__(self, rabbitmq_hosts=None, scrape_interval=15, 
                metrics_port=None, prometheus_endpoint='/metrics'):
        """
        Initialize the monitoring service.
        
        Args:
            rabbitmq_hosts: List of RabbitMQ hosts to monitor (default: localhost)
            scrape_interval: Interval in seconds between scrapes (default: 15)
            metrics_port: Port to expose Prometheus metrics on (default: from config)
            prometheus_endpoint: RabbitMQ Prometheus endpoint
        """
        self.rabbitmq_hosts = rabbitmq_hosts or ['localhost']
        self.scrape_interval = scrape_interval
        self.metrics_port = metrics_port or config.METRICS_PORT
        self.prometheus_endpoint = prometheus_endpoint
        self._stop_event = threading.Event()
        self._thread = None
        
        # Prometheus metrics
        self.queue_messages = Gauge('rabbitmq_queue_messages', 
                                  'Number of messages in a queue',
                                  ['queue', 'vhost'])
                                  
        self.queue_consumers = Gauge('rabbitmq_queue_consumers',
                                   'Number of consumers for a queue',
                                   ['queue', 'vhost'])
                                   
        self.exchange_messages_published = Counter('rabbitmq_exchange_messages_published_total',
                                               'Total number of messages published to an exchange',
                                               ['exchange', 'vhost'])
                                               
        self.node_up = Gauge('rabbitmq_node_up',
                          'Whether the RabbitMQ node is running',
                          ['node'])
                          
        self.connection_count = Gauge('rabbitmq_connections',
                                   'Number of connections to RabbitMQ')
                                   
        self.channel_count = Gauge('rabbitmq_channels',
                                'Number of channels in RabbitMQ')
        
    def start(self):
        """Start the monitoring service."""
        if self._thread and self._thread.is_alive():
            logger.warning("Monitoring service already running")
            return
            
        logger.info(f"Starting monitoring service on port {self.metrics_port}")
            
        # Start Prometheus HTTP server in the background
        try:
            start_http_server(self.metrics_port)
        except Exception as e:
            logger.error(f"Failed to start Prometheus HTTP server: {e}")
            return
            
        # Start scraping in a separate thread
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._scrape_loop)
        self._thread.daemon = True
        self._thread.start()
        
    def stop(self):
        """Stop the monitoring service."""
        if not self._thread or not self._thread.is_alive():
            logger.warning("Monitoring service not running")
            return
            
        logger.info("Stopping monitoring service")
        self._stop_event.set()
        self._thread.join(timeout=self.scrape_interval + 5)
        if self._thread.is_alive():
            logger.warning("Failed to stop monitoring thread cleanly")
        
    def _scrape_loop(self):
        """Main loop for scraping RabbitMQ metrics."""
        while not self._stop_event.is_set():
            try:
                self._scrape_metrics()
            except Exception as e:
                logger.error(f"Error scraping metrics: {e}")
                
            # Sleep until next scrape interval or stop event
            self._stop_event.wait(self.scrape_interval)
            
    def _scrape_metrics(self):
        """Scrape metrics from all RabbitMQ hosts."""
        for host in self.rabbitmq_hosts:
            try:
                self._scrape_host_metrics(host)
            except Exception as e:
                logger.error(f"Error scraping metrics from {host}: {e}")
                self.node_up.labels(node=host).set(0)
                
    def _scrape_host_metrics(self, host):
        """
        Scrape metrics from a single RabbitMQ host.
        
        Args:
            host: Hostname of the RabbitMQ instance
        """
        logger.debug(f"Scraping metrics from {host}")
        
        # Mark node as up
        self.node_up.labels(node=host).set(1)
        
        # Try to access RabbitMQ's Prometheus endpoint
        try:
            metrics_url = f"http://{host}:15692{self.prometheus_endpoint}"
            response = requests.get(metrics_url, timeout=5)
            response.raise_for_status()
            
            # Process raw Prometheus metrics
            # This is a simple approach; a more robust solution would parse the Prometheus format
            raw_metrics = response.text
            logger.debug(f"Received {len(raw_metrics)} bytes of metrics from {host}")
            
            # Extract some basic metrics
            self._extract_basic_metrics(raw_metrics, host)
            
        except requests.RequestException:
            # Fall back to RabbitMQ HTTP API
            logger.debug(f"Prometheus endpoint not available for {host}, trying HTTP API")
            self._scrape_from_http_api(host)
            
    def _extract_basic_metrics(self, raw_metrics, host):
        """
        Extract basic metrics from raw Prometheus metrics.
        
        Args:
            raw_metrics: Raw Prometheus metrics text
            host: Hostname of the RabbitMQ instance
        """
        # Count connections
        connections = 0
        for line in raw_metrics.split('\n'):
            if 'rabbitmq_connections' in line and not line.startswith('#'):
                try:
                    parts = line.split()
                    if len(parts) >= 2:
                        connections = int(float(parts[1]))
                        break
                except (ValueError, IndexError):
                    pass
                    
        self.connection_count.set(connections)
        
        # Count channels (similar approach)
        channels = 0
        for line in raw_metrics.split('\n'):
            if 'rabbitmq_channels' in line and not line.startswith('#'):
                try:
                    parts = line.split()
                    if len(parts) >= 2:
                        channels = int(float(parts[1]))
                        break
                except (ValueError, IndexError):
                    pass
                    
        self.channel_count.set(channels)
        
        # Process queue metrics
        for line in raw_metrics.split('\n'):
            if 'rabbitmq_queue_messages' in line and not line.startswith('#'):
                try:
                    # This is a simple parser and may not handle all cases correctly
                    parts = line.split('{')[1].split('}')[0].split(',')
                    labels = {}
                    for part in parts:
                        k, v = part.split('=')
                        labels[k.strip()] = v.strip('"')
                        
                    value_part = line.split('}')[1].strip()
                    value = float(value_part)
                    
                    queue = labels.get('queue', '')
                    vhost = labels.get('vhost', '/')
                    
                    if queue:
                        self.queue_messages.labels(queue=queue, vhost=vhost).set(value)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing queue metrics: {e}")
        
    def _scrape_from_http_api(self, host):
        """
        Scrape metrics from RabbitMQ HTTP API.
        
        Args:
            host: Hostname of the RabbitMQ instance
        """
        try:
            # Queues
            queues_url = f"http://{host}:15672/api/queues"
            auth = (config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
            response = requests.get(queues_url, auth=auth, timeout=5)
            response.raise_for_status()
            
            queues = response.json()
            logger.debug(f"Retrieved {len(queues)} queues from {host}")
            
            for queue in queues:
                queue_name = queue.get('name', '')
                vhost = queue.get('vhost', '/')
                messages = queue.get('messages', 0)
                consumers = queue.get('consumers', 0)
                
                self.queue_messages.labels(queue=queue_name, vhost=vhost).set(messages)
                self.queue_consumers.labels(queue=queue_name, vhost=vhost).set(consumers)
                
            # Connections
            connections_url = f"http://{host}:15672/api/connections"
            response = requests.get(connections_url, auth=auth, timeout=5)
            response.raise_for_status()
            
            connections = response.json()
            self.connection_count.set(len(connections))
            
            # Channels
            channels_url = f"http://{host}:15672/api/channels"
            response = requests.get(channels_url, auth=auth, timeout=5)
            response.raise_for_status()
            
            channels = response.json()
            self.channel_count.set(len(channels))
            
        except requests.RequestException as e:
            logger.error(f"Error accessing HTTP API on {host}: {e}")
            raise

# Singleton instance
monitoring_service = MonitoringService()