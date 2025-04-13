#!/usr/bin/env python3
"""
init_rabbitmq_project.py

Create the standard RabbitMQ project directory structure with placeholder files.
"""

import argparse
from pathlib import Path

# Define your project layout here
PROJECT_STRUCTURE = {
    "config": ["config.py"],
    "core": [
        "connection_manager.py",
        "channel_manager.py",
        "retry_handler.py",
        "cluster_manager.py",
        "logger.py",
    ],
    "exchanges": ["exchange_manager.py"],
    "queues": ["queue_manager.py"],
    "bindings": ["binding_manager.py"],
    "producers": ["base_producer.py", "order_producer.py"],
    "consumers": ["base_consumer.py", "inventory_consumer.py"],
    "services": ["message_router.py", "monitoring.py"],
    "utils": ["metrics_collector.py"],
    "tests": ["test_producer.py", "test_consumer.py", "test_retry_handler.py"],
}

ROOT_FILES = [
    "docker-compose.yml",
    "requirements.txt",
    "README.md",
]


def create_project_structure(base_dir: Path):
    """
    Create directories and files under base_dir according to PROJECT_STRUCTURE.
    """
    # 1) Create subdirectories and touch files inside them
    for folder, files in PROJECT_STRUCTURE.items():
        folder_path = base_dir / folder
        folder_path.mkdir(parents=True, exist_ok=True)
        for filename in files:
            file_path = folder_path / filename
            file_path.touch(exist_ok=True)

    # 2) Touch the root‑level files
    for filename in ROOT_FILES:
        (base_dir / filename).touch(exist_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Initialize a RabbitMQ project directory structure."
    )
    parser.add_argument(
        "project_root",
        nargs="?",
        default="rabbitmq_project",
        help="Name of the root project directory to create",
    )
    args = parser.parse_args()

    base_dir = Path(args.project_root).resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    create_project_structure(base_dir)

    print(f"Initialized RabbitMQ project structure under: {base_dir}")


if __name__ == "__main__":
    main()
