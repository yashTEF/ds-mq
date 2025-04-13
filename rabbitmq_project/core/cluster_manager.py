"""
Cluster manager for RabbitMQ clustering operations.
Provides methods to set up and manage RabbitMQ clusters.
"""
import subprocess
import time
import os
import requests
from ..config.config import config
from .logger import get_logger

# Module logger
logger = get_logger(__name__)

class ClusterManager:
    """
    Manages RabbitMQ cluster formation and operations.
    Handles node join/leave operations and cluster status checks.
    """
    
    def __init__(self, erlang_cookie=None):
        """
        Initialize cluster manager.
        
        Args:
            erlang_cookie: The Erlang cookie for cluster authentication (default from env)
        """
        self.erlang_cookie = erlang_cookie or os.environ.get('RABBITMQ_ERLANG_COOKIE', 'secretcookie')
        
    def run_rabbitmqctl(self, node_name, command, args=None):
        """
        Run a rabbitmqctl command on a specific node.
        
        Args:
            node_name: The name of the node (e.g., 'rabbit@rabbit1')
            command: The rabbitmqctl command to run
            args: Additional arguments for the command
            
        Returns:
            The command output as a string
        """
        cmd = ['docker', 'exec', node_name.split('@')[1], 'rabbitmqctl']
        
        if '@' in node_name:
            cmd.extend(['-n', node_name])
            
        cmd.append(command)
        
        if args:
            if isinstance(args, list):
                cmd.extend(args)
            else:
                cmd.append(args)
                
        logger.info(f"Running command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd, 
                check=True, 
                capture_output=True, 
                text=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with code {e.returncode}: {e.stderr}")
            raise
            
    def stop_app(self, node_name):
        """
        Stop the RabbitMQ app on a node.
        
        Args:
            node_name: The name of the node
        """
        logger.info(f"Stopping RabbitMQ app on {node_name}")
        return self.run_rabbitmqctl(node_name, 'stop_app')
        
    def start_app(self, node_name):
        """
        Start the RabbitMQ app on a node.
        
        Args:
            node_name: The name of the node
        """
        logger.info(f"Starting RabbitMQ app on {node_name}")
        return self.run_rabbitmqctl(node_name, 'start_app')
        
    def reset(self, node_name):
        """
        Reset a RabbitMQ node to clean state.
        
        Args:
            node_name: The name of the node
        """
        logger.info(f"Resetting node {node_name}")
        return self.run_rabbitmqctl(node_name, 'reset')
        
    def join_cluster(self, node_name, target_node, node_type='disc'):
        """
        Join a node to a cluster.
        
        Args:
            node_name: The name of the node joining the cluster
            target_node: The name of the target node in the cluster
            node_type: The node type ('disc' or 'ram')
        """
        logger.info(f"Joining {node_name} to cluster with {target_node} as {node_type} node")
        
        # Stop app
        self.stop_app(node_name)
        
        # Reset node
        self.reset(node_name)
        
        # Join cluster with specific node type
        try:
            if node_type == 'ram':
                return self.run_rabbitmqctl(
                    node_name, 
                    'join_cluster', 
                    [f"--ram", target_node]
                )
            else:
                return self.run_rabbitmqctl(
                    node_name, 
                    'join_cluster', 
                    target_node
                )
        finally:
            # Start app regardless of join result
            self.start_app(node_name)
            
    def cluster_status(self, node_name='rabbit@rabbit1'):
        """
        Get the status of the RabbitMQ cluster.
        
        Args:
            node_name: The name of the node to query
            
        Returns:
            The cluster status output
        """
        logger.info(f"Getting cluster status from {node_name}")
        return self.run_rabbitmqctl(node_name, 'cluster_status')
        
    def set_policy(self, node_name, name, pattern, definition, priority=0, apply_to='all'):
        """
        Set a policy on a RabbitMQ node.
        
        Args:
            node_name: The name of the node
            name: The policy name
            pattern: The pattern to match for applying the policy
            definition: The policy definition (JSON string)
            priority: The policy priority
            apply_to: What to apply the policy to ('queues', 'exchanges', 'all')
        """
        logger.info(f"Setting policy {name} on {node_name}")
        return self.run_rabbitmqctl(
            node_name,
            'set_policy',
            [name, pattern, definition, f"--priority={priority}", f"--apply-to={apply_to}"]
        )
        
    def setup_cluster(self, nodes, primary_node='rabbit@rabbit1'):
        """
        Set up a RabbitMQ cluster with the given nodes.
        
        Args:
            nodes: List of node names to include in the cluster
            primary_node: The primary node in the cluster
        """
        logger.info(f"Setting up cluster with nodes: {nodes}")
        
        if not nodes or primary_node not in nodes:
            raise ValueError("Primary node must be in the list of nodes")
            
        # Start with primary node
        try:
            # Ensure primary node is started
            self.start_app(primary_node)
            logger.info(f"Primary node {primary_node} is ready")
            
            # Join other nodes to the primary
            for node in nodes:
                if node != primary_node:
                    try:
                        self.join_cluster(node, primary_node)
                        logger.info(f"Node {node} joined the cluster")
                    except Exception as e:
                        logger.error(f"Failed to join {node} to cluster: {e}")
                        
            # Verify cluster formation
            status = self.cluster_status(primary_node)
            logger.info(f"Cluster status: {status}")
            
            # Setup HA policy for queues if multiple nodes
            if len(nodes) > 1:
                ha_definition = '{"ha-mode":"all", "ha-sync-mode":"automatic"}'
                try:
                    self.set_policy(
                        primary_node,
                        'ha-all',
                        '.*',  # Apply to all queues
                        ha_definition,
                        priority=0,
                        apply_to='queues'
                    )
                    logger.info("HA policy applied to all queues")
                except Exception as e:
                    logger.error(f"Failed to set HA policy: {e}")
                    
            # Setup quorum queue policy
            quorum_definition = '{"x-queue-type":"quorum"}'
            try:
                self.set_policy(
                    primary_node,
                    'quorum-queues',
                    '^quorum\\.',  # Apply to queues starting with 'quorum.'
                    quorum_definition,
                    priority=1,
                    apply_to='queues'
                )
                logger.info("Quorum queue policy applied")
            except Exception as e:
                logger.error(f"Failed to set quorum queue policy: {e}")
                
            return True
            
        except Exception as e:
            logger.error(f"Cluster setup failed: {e}")
            return False
            
    def is_cluster_healthy(self, nodes):
        """
        Check if the RabbitMQ cluster is healthy.
        
        Args:
            nodes: List of node names to check
            
        Returns:
            True if all nodes are in the cluster and running, False otherwise
        """
        try:
            # Check cluster status from first node
            status_output = self.cluster_status(nodes[0])
            
            # Check if all nodes are in the status output
            for node in nodes:
                if node not in status_output:
                    logger.warning(f"Node {node} is not in the cluster")
                    return False
                    
            # Check if each node is running
            for node in nodes:
                try:
                    status = self.run_rabbitmqctl(node, 'status')
                    if "running_applications" not in status:
                        logger.warning(f"Node {node} is not running")
                        return False
                except Exception as e:
                    logger.warning(f"Failed to check status of {node}: {e}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Cluster health check failed: {e}")
            return False

# Singleton instance
cluster_manager = ClusterManager()