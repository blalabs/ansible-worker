"""MQTT client for receiving tasks and publishing status updates."""

import json
import logging
import ssl
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt

from ansible_worker.config import MQTTConfig
from ansible_worker.models import TaskStatus

logger = logging.getLogger(__name__)


class MQTTClient:
    """MQTT client for task communication."""

    def __init__(
        self,
        config: MQTTConfig,
        group_name: str,
        topic_prefix: str,
        on_task_message: Callable[[dict[str, Any]], None],
    ) -> None:
        """Initialize the MQTT client.

        Args:
            config: MQTT configuration.
            group_name: Worker group name for topic subscription.
            topic_prefix: Prefix for MQTT topics (e.g., "ansible").
            on_task_message: Callback for incoming task messages.
        """
        self._config = config
        self._group_name = group_name
        self._topic_prefix = topic_prefix
        self._on_task_message = on_task_message
        self._connected = threading.Event()
        self._shutdown = False

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        if config.username and config.password:
            self._client.username_pw_set(config.username, config.password)

        if config.tls_enabled:
            self._client.tls_set(
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
            )

    @property
    def task_topic(self) -> str:
        """Get the base task topic (without shared subscription prefix)."""
        return f"{self._topic_prefix}/{self._group_name}/tasks"

    @property
    def _shared_task_topic(self) -> str:
        """Get the shared subscription topic for tasks.

        Uses MQTT 5.0 shared subscriptions: $share/<group>/<topic>
        The broker delivers each message to only ONE subscriber in the group.
        """
        return f"$share/ansible-worker-{self._group_name}/{self._topic_prefix}/{self._group_name}/tasks"

    def status_topic(self, task_id: str) -> str:
        """Get the status topic for a specific task."""
        return f"{self._topic_prefix}/{self._group_name}/tasks/{task_id}/status"

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        """Handle connection established."""
        if reason_code == mqtt.CONNACK_ACCEPTED:
            logger.info(f"Connected to MQTT broker at {self._config.host}:{self._config.port}")
            # Subscribe using shared subscription for distributed task handling
            client.subscribe(self._shared_task_topic, qos=2)
            logger.info(f"Subscribed to {self._shared_task_topic}")
            self._connected.set()
        else:
            logger.error(f"Connection failed with reason code: {reason_code}")

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        """Handle disconnection."""
        self._connected.clear()
        if self._shutdown:
            logger.info("Disconnected from MQTT broker")
        else:
            logger.warning(f"Disconnected from MQTT broker: {reason_code}")

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        """Handle incoming message."""
        logger.debug(f"Received message on {message.topic}")

        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            return
        except UnicodeDecodeError as e:
            logger.error(f"Invalid UTF-8 in message: {e}")
            return

        try:
            self._on_task_message(payload)
        except Exception as e:
            logger.exception(f"Error processing task message: {e}")

    def connect(self, timeout: float = 30.0) -> bool:
        """Connect to the MQTT broker.

        Args:
            timeout: Connection timeout in seconds.

        Returns:
            True if connection was successful, False otherwise.
        """
        try:
            self._client.connect(
                self._config.host,
                self._config.port,
                keepalive=self._config.keepalive,
            )
            self._client.loop_start()

            if self._connected.wait(timeout):
                return True

            logger.error("Connection timed out")
            return False

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        self._shutdown = True
        self._client.loop_stop()
        self._client.disconnect()

    def publish_status(self, status: TaskStatus) -> None:
        """Publish a task status update.

        Args:
            status: The task status to publish.
        """
        topic = self.status_topic(status.task_id)
        payload = json.dumps(status.to_dict())

        result = self._client.publish(
            topic,
            payload,
            qos=1,
            retain=True,
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.debug(f"Published status for task {status.task_id}: {status.state.value}")
        else:
            logger.error(f"Failed to publish status for task {status.task_id}: {result.rc}")

    def wait_for_connection(self, timeout: float | None = None) -> bool:
        """Wait for connection to be established.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            True if connected, False if timeout occurred.
        """
        return self._connected.wait(timeout)

    @property
    def is_connected(self) -> bool:
        """Check if currently connected to the broker."""
        return self._connected.is_set()
