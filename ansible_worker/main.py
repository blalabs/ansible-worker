"""Main application orchestrator for Ansible MQTT Worker."""

import argparse
import logging
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from ansible_worker.config import Config
from ansible_worker.executor import Executor
from ansible_worker.models import Task, TaskRequest, TaskState, TaskStatus
from ansible_worker.mqtt_client import MQTTClient
from ansible_worker.task_queue import QueueFullError, TaskQueue

logger = logging.getLogger(__name__)


class AnsibleWorker:
    """Main application orchestrating MQTT, queue, and executor."""

    def __init__(self, config: Config) -> None:
        """Initialize the worker.

        Args:
            config: Application configuration.
        """
        self._config = config
        self._shutdown_event = threading.Event()

        self._task_queue = TaskQueue(max_size=config.worker.max_queue_size)

        self._mqtt_client = MQTTClient(
            config=config.mqtt,
            group_name=config.worker.group_name,
            topic_prefix=config.worker.topic_prefix,
            on_task_message=self._handle_task_message,
        )

        self._executor = Executor(
            config=config.worker,
            task_queue=self._task_queue,
            on_status_update=self._publish_status,
        )

    def _handle_task_message(self, payload: dict[str, Any]) -> None:
        """Handle incoming task message from MQTT.

        Args:
            payload: Parsed JSON payload from the message.
        """
        try:
            request = TaskRequest.from_dict(payload)
        except ValueError as e:
            logger.error(f"Invalid task request: {e}")
            return

        task = Task.create(request)
        logger.info(f"Created task {task.task_id} for playbook {request.playbook}")

        try:
            self._task_queue.put(task)
            self._publish_status(task.status)
        except QueueFullError:
            logger.warning(f"Queue full, rejecting task {task.task_id}")
            task.status.state = TaskState.FAILED
            task.status.error_message = "Task queue is full"
            self._publish_status(task.status)

    def _publish_status(self, status: TaskStatus) -> None:
        """Publish task status to MQTT.

        Args:
            status: The task status to publish.
        """
        self._mqtt_client.publish_status(status)

    def run(self) -> int:
        """Run the worker application.

        Returns:
            Exit code (0 for success, non-zero for failure).
        """
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        logger.info("Starting Ansible MQTT Worker")
        logger.info(f"Group: {self._config.worker.group_name}")
        logger.info(f"Topic prefix: {self._config.worker.topic_prefix}")
        logger.info(f"Playbook directory: {self._config.worker.playbook_directory}")

        if not self._mqtt_client.connect():
            logger.error("Failed to connect to MQTT broker")
            return 1

        self._executor.start()

        logger.info("Worker is ready and waiting for tasks")

        self._shutdown_event.wait()

        logger.info("Shutting down...")
        self._executor.stop()

        remaining_tasks = self._task_queue.clear()
        for task in remaining_tasks:
            task.status.state = TaskState.CANCELLED
            task.status.error_message = "Worker shutdown"
            self._publish_status(task.status)

        self._mqtt_client.disconnect()
        logger.info("Shutdown complete")

        return 0

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating shutdown")
        self._shutdown_event.set()


def setup_logging(log_level: str) -> None:
    """Configure logging for the application.

    Args:
        log_level: Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Ansible MQTT Worker - Execute Ansible playbooks via MQTT",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate configuration and exit",
    )
    return parser.parse_args()


def main() -> int:
    """Main entry point.

    Returns:
        Exit code.
    """
    args = parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {args.config}", file=sys.stderr)
        return 1

    try:
        config = Config.load(config_path)
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1

    setup_logging(config.log_level)

    try:
        config.validate()
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        return 1

    if args.validate:
        print("Configuration is valid")
        return 0

    worker = AnsibleWorker(config)
    return worker.run()


if __name__ == "__main__":
    sys.exit(main())
