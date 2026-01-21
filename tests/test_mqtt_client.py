"""Tests for ansible_worker.mqtt_client."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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
        assert client._shared_task_topic == "$share/ansible-worker-production/ansible/production/tasks"

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
        mock_message.payload = b'{"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}'

        client._on_message(mock_paho_client, None, mock_message)

        assert len(received_messages) == 1
        assert received_messages[0] == {"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}

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

    def test_on_message_callback_exception(self, mqtt_config, mock_paho_client):
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
        mock_message.payload = b'{"task_id": "task123", "playbook": "site.yml", "inventory": "prod"}'

        # Should not raise, just log error
        client._on_message(mock_paho_client, None, mock_message)
