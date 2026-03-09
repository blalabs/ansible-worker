"""Tests for ansible_worker.mqtt_client."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt
import pytest

from ansible_worker.config import MQTTConfig
from ansible_worker.models import TaskState, TaskStatus
from ansible_worker.mqtt_client import MQTTClient


@pytest.fixture
def mqtt_config() -> MQTTConfig:
    """Create a test MQTT configuration."""
    return MQTTConfig(
        host="localhost",
        port=1883,
        username="testuser",
        password="testpass",
    )


@pytest.fixture
def mock_paho_client():
    """Create a mock paho MQTT client."""
    with patch("ansible_worker.mqtt_client.mqtt.Client") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


class TestMQTTClient:
    """Tests for MQTTClient."""

    def test_task_topic(self, mqtt_config, mock_paho_client):
        """Test task topic generation."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="production",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        assert client.task_topic == "ansible/production/tasks"

    def test_task_topic_custom_prefix(self, mqtt_config, mock_paho_client):
        """Test task topic with custom prefix."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="staging",
            topic_prefix="myapp",
            on_task_message=lambda x: None,
        )
        assert client.task_topic == "myapp/staging/tasks"

    def test_shared_task_topic(self, mqtt_config, mock_paho_client):
        """Test shared subscription topic generation."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="production",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        # Format: $share/ansible-worker-<group>/<topic>
        expected = "$share/ansible-worker-production/ansible/production/tasks"
        assert client._shared_task_topic == expected

    def test_shared_task_topic_custom_prefix(self, mqtt_config, mock_paho_client):
        """Test shared subscription topic with custom prefix."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="staging",
            topic_prefix="myapp",
            on_task_message=lambda x: None,
        )
        assert client._shared_task_topic == "$share/ansible-worker-staging/myapp/staging/tasks"

    def test_status_topic(self, mqtt_config, mock_paho_client):
        """Test status topic generation."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="production",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        assert client.status_topic("abc123") == "ansible/production/tasks/abc123/status"

    def test_status_topic_custom_prefix(self, mqtt_config, mock_paho_client):
        """Test status topic with custom prefix."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="dev",
            topic_prefix="custom",
            on_task_message=lambda x: None,
        )
        assert client.status_topic("xyz789") == "custom/dev/tasks/xyz789/status"

    def test_credentials_set(self, mqtt_config, mock_paho_client):
        """Test that credentials are set on the client."""
        MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        mock_paho_client.username_pw_set.assert_called_once_with("testuser", "testpass")

    def test_no_credentials_when_not_provided(self, mock_paho_client):
        """Test that credentials are not set when not provided."""
        config = MQTTConfig(host="localhost")
        MQTTClient(
            config=config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        mock_paho_client.username_pw_set.assert_not_called()

    def test_tls_enabled(self, mock_paho_client):
        """Test that TLS is configured when enabled."""
        config = MQTTConfig(host="localhost", tls_enabled=True)
        MQTTClient(
            config=config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        mock_paho_client.tls_set.assert_called_once()

    def test_tls_disabled(self, mqtt_config, mock_paho_client):
        """Test that TLS is not configured when disabled."""
        MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        mock_paho_client.tls_set.assert_not_called()

    def test_publish_status(self, mqtt_config, mock_paho_client):
        """Test publishing a task status."""
        mock_paho_client.publish.return_value = MagicMock(rc=0)

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        status = TaskStatus(
            task_id="abc123",
            state=TaskState.RUNNING,
            created_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        )

        client.publish_status(status)

        mock_paho_client.publish.assert_called_once()
        call_args = mock_paho_client.publish.call_args
        assert call_args[0][0] == "ansible/test/tasks/abc123/status"
        assert call_args[1]["qos"] == 1
        assert call_args[1]["retain"] is True

        # Verify payload is valid JSON
        payload = json.loads(call_args[0][1])
        assert payload["task_id"] == "abc123"
        assert payload["state"] == "running"

    def test_disconnect(self, mqtt_config, mock_paho_client):
        """Test disconnecting from broker."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        client.disconnect()

        mock_paho_client.loop_stop.assert_called_once()
        mock_paho_client.disconnect.assert_called_once()

    def test_is_connected_initially_false(self, mqtt_config, mock_paho_client):
        """Test that is_connected is False initially."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        assert client.is_connected is False


class TestMQTTClientMessageHandling:
    """Tests for MQTT message handling."""

    def test_on_message_valid_json(self, mqtt_config, mock_paho_client):
        """Test handling a valid JSON message."""
        received_messages = []

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda msg: received_messages.append(msg),
        )

        # Simulate receiving a message
        mock_message = MagicMock()
        mock_message.topic = "ansible/test/tasks"
        mock_message.payload = (
            b'{"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}'
        )

        client._on_message(mock_paho_client, None, mock_message)

        expected = {"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}
        assert len(received_messages) == 1
        assert received_messages[0] == expected

    def test_on_message_invalid_json(self, mqtt_config, mock_paho_client):
        """Test handling an invalid JSON message."""
        received_messages = []

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda msg: received_messages.append(msg),
        )

        mock_message = MagicMock()
        mock_message.topic = "ansible/test/tasks"
        mock_message.payload = b"not valid json"

        # Should not raise, just log error
        client._on_message(mock_paho_client, None, mock_message)

        assert len(received_messages) == 0

    def test_on_message_invalid_utf8(self, mqtt_config, mock_paho_client):
        """Test handling a message with invalid UTF-8."""
        received_messages = []

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda msg: received_messages.append(msg),
        )

        mock_message = MagicMock()
        mock_message.topic = "ansible/test/tasks"
        mock_message.payload = b"\xff\xfe"  # Invalid UTF-8

        # Should not raise, just log error
        client._on_message(mock_paho_client, None, mock_message)

        assert len(received_messages) == 0

    def test_on_message_callback_exception_logged(self, mqtt_config, mock_paho_client):
        """Test that exceptions in callback are caught."""
        def failing_callback(msg):
            raise ValueError("Test error")

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=failing_callback,
        )

        mock_message = MagicMock()
        mock_message.topic = "ansible/test/tasks"
        mock_message.payload = (
            b'{"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}'
        )

        # Should not raise, just log error
        client._on_message(mock_paho_client, None, mock_message)


class TestMQTTClientConnection:
    """Tests for MQTT connection handling."""

    def test_on_connect_success(self, mqtt_config, mock_paho_client):
        """Test _on_connect subscribes and sets connected on CONNACK_ACCEPTED."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        client._on_connect(mock_paho_client, None, None, mqtt.CONNACK_ACCEPTED, None)

        mock_paho_client.subscribe.assert_called_once_with(
            client._shared_task_topic, qos=2
        )
        assert client.is_connected is True

    def test_on_connect_failure(self, mqtt_config, mock_paho_client):
        """Test _on_connect logs error on non-accepted reason code."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        client._on_connect(mock_paho_client, None, None, 5, None)

        mock_paho_client.subscribe.assert_not_called()
        assert client.is_connected is False

    def test_on_disconnect_graceful(self, mqtt_config, mock_paho_client):
        """Test _on_disconnect with shutdown=True logs info."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        # Simulate being connected first
        client._connected.set()
        client._shutdown = True

        with patch("ansible_worker.mqtt_client.logger") as mock_logger:
            client._on_disconnect(mock_paho_client, None, None, 0, None)

        assert client.is_connected is False
        mock_logger.info.assert_called_once_with("Disconnected from MQTT broker")

    def test_on_disconnect_unexpected(self, mqtt_config, mock_paho_client):
        """Test _on_disconnect with shutdown=False logs warning."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        client._connected.set()
        client._shutdown = False

        with patch("ansible_worker.mqtt_client.logger") as mock_logger:
            client._on_disconnect(mock_paho_client, None, None, 16, None)

        assert client.is_connected is False
        mock_logger.warning.assert_called_once()

    def test_connect_success(self, mqtt_config, mock_paho_client):
        """Test connect() happy path."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        # Simulate the connected event being set after connect/loop_start
        def set_connected(*args, **kwargs):
            client._connected.set()

        mock_paho_client.connect.side_effect = set_connected

        result = client.connect(timeout=5.0)

        assert result is True
        mock_paho_client.connect.assert_called_once_with(
            mqtt_config.host, mqtt_config.port, keepalive=mqtt_config.keepalive
        )
        mock_paho_client.loop_start.assert_called_once()

    def test_connect_timeout(self, mqtt_config, mock_paho_client):
        """Test connect() when connected event is never set."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        result = client.connect(timeout=0.1)

        assert result is False

    def test_connect_exception(self, mqtt_config, mock_paho_client):
        """Test connect() when _client.connect() raises."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )
        mock_paho_client.connect.side_effect = ConnectionRefusedError("refused")

        result = client.connect(timeout=5.0)

        assert result is False

    def test_publish_status_failure(self, mqtt_config, mock_paho_client):
        """Test publish_status when publish returns non-success rc."""
        mock_paho_client.publish.return_value = MagicMock(rc=1)

        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        status = TaskStatus(
            task_id="fail123",
            state=TaskState.RUNNING,
            created_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        )

        with patch("ansible_worker.mqtt_client.logger") as mock_logger:
            client.publish_status(status)

        mock_logger.error.assert_called_once()
        assert "fail123" in mock_logger.error.call_args[0][0]

    def test_wait_for_connection(self, mqtt_config, mock_paho_client):
        """Test wait_for_connection returns correct value."""
        client = MQTTClient(
            config=mqtt_config,
            group_name="test",
            topic_prefix="ansible",
            on_task_message=lambda x: None,
        )

        # Not connected - should return False with short timeout
        assert client.wait_for_connection(timeout=0.05) is False

        # Set connected - should return True
        client._connected.set()
        assert client.wait_for_connection(timeout=0.05) is True
