"""Tests for ansible_worker.config."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from ansible_worker.config import (
    Config,
    MQTTConfig,
    WorkerConfig,
    expand_env_vars,
)


class TestExpandEnvVars:
    """Tests for environment variable expansion."""

    def test_expand_string(self, monkeypatch):
        """Test expanding env var in a string."""
        monkeypatch.setenv("TEST_VAR", "hello")
        result = expand_env_vars("${TEST_VAR} world")
        assert result == "hello world"

    def test_expand_multiple(self, monkeypatch):
        """Test expanding multiple env vars."""
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "1883")
        result = expand_env_vars("${HOST}:${PORT}")
        assert result == "localhost:1883"

    def test_expand_in_dict(self, monkeypatch):
        """Test expanding env vars in a dictionary."""
        monkeypatch.setenv("PASSWORD", "secret")
        data = {"username": "user", "password": "${PASSWORD}"}
        result = expand_env_vars(data)
        assert result == {"username": "user", "password": "secret"}

    def test_expand_in_list(self, monkeypatch):
        """Test expanding env vars in a list."""
        monkeypatch.setenv("ITEM", "value")
        data = ["static", "${ITEM}"]
        result = expand_env_vars(data)
        assert result == ["static", "value"]

    def test_expand_nested(self, monkeypatch):
        """Test expanding env vars in nested structures."""
        monkeypatch.setenv("SECRET", "mysecret")
        data = {"outer": {"inner": "${SECRET}"}}
        result = expand_env_vars(data)
        assert result == {"outer": {"inner": "mysecret"}}

    def test_missing_env_var(self):
        """Test that missing env var raises ValueError."""
        with pytest.raises(ValueError, match="Environment variable not set: NONEXISTENT"):
            expand_env_vars("${NONEXISTENT}")

    def test_no_expansion_needed(self):
        """Test strings without env vars pass through."""
        result = expand_env_vars("plain string")
        assert result == "plain string"

    def test_non_string_passthrough(self):
        """Test that non-string types pass through unchanged."""
        assert expand_env_vars(42) == 42
        assert expand_env_vars(3.14) == 3.14
        assert expand_env_vars(True) is True
        assert expand_env_vars(None) is None


class TestMQTTConfig:
    """Tests for MQTTConfig."""

    def test_from_dict_minimal(self):
        """Test creating MQTTConfig with only required fields."""
        data = {"host": "mqtt.example.com"}
        config = MQTTConfig.from_dict(data)

        assert config.host == "mqtt.example.com"
        assert config.port == 1883
        assert config.username is None
        assert config.password is None
        assert config.keepalive == 60
        assert config.tls_enabled is False

    def test_from_dict_full(self):
        """Test creating MQTTConfig with all fields."""
        data = {
            "host": "mqtt.example.com",
            "port": 8883,
            "username": "user",
            "password": "pass",
            "keepalive": 120,
            "tls_enabled": True,
        }
        config = MQTTConfig.from_dict(data)

        assert config.host == "mqtt.example.com"
        assert config.port == 8883
        assert config.username == "user"
        assert config.password == "pass"
        assert config.keepalive == 120
        assert config.tls_enabled is True

    def test_from_dict_missing_host(self):
        """Test that missing host raises ValueError."""
        data = {"port": 1883}
        with pytest.raises(ValueError, match="missing required field: host"):
            MQTTConfig.from_dict(data)


class TestWorkerConfig:
    """Tests for WorkerConfig."""

    def test_from_dict_minimal(self):
        """Test creating WorkerConfig with only required fields."""
        data = {
            "group_name": "production",
            "playbook_directory": "/opt/playbooks",
        }
        config = WorkerConfig.from_dict(data)

        assert config.group_name == "production"
        assert config.playbook_directory == "/opt/playbooks"
        assert config.topic_prefix == "ansible"
        assert config.max_queue_size == 100
        assert config.task_timeout == 3600

    def test_from_dict_full(self):
        """Test creating WorkerConfig with all fields."""
        data = {
            "group_name": "staging",
            "playbook_directory": "/var/ansible",
            "topic_prefix": "myapp",
            "max_queue_size": 50,
            "task_timeout": 7200,
        }
        config = WorkerConfig.from_dict(data)

        assert config.group_name == "staging"
        assert config.playbook_directory == "/var/ansible"
        assert config.topic_prefix == "myapp"
        assert config.max_queue_size == 50
        assert config.task_timeout == 7200

    def test_from_dict_missing_group_name(self):
        """Test that missing group_name raises ValueError."""
        data = {"playbook_directory": "/opt/playbooks"}
        with pytest.raises(ValueError, match="missing required field: group_name"):
            WorkerConfig.from_dict(data)

    def test_from_dict_missing_playbook_directory(self):
        """Test that missing playbook_directory raises ValueError."""
        data = {"group_name": "production"}
        with pytest.raises(ValueError, match="missing required field: playbook_directory"):
            WorkerConfig.from_dict(data)

    def test_validate_directory_exists(self, temp_playbook_dir):
        """Test validation passes for existing directory."""
        config = WorkerConfig(
            group_name="test",
            playbook_directory=str(temp_playbook_dir),
        )
        config.validate()  # Should not raise

    def test_validate_directory_not_exists(self):
        """Test validation fails for non-existent directory."""
        config = WorkerConfig(
            group_name="test",
            playbook_directory="/nonexistent/path",
        )
        with pytest.raises(ValueError, match="does not exist"):
            config.validate()

    def test_validate_not_a_directory(self, temp_playbook_dir):
        """Test validation fails when path is not a directory."""
        file_path = temp_playbook_dir / "file.txt"
        file_path.touch()

        config = WorkerConfig(
            group_name="test",
            playbook_directory=str(file_path),
        )
        with pytest.raises(ValueError, match="not a directory"):
            config.validate()


class TestConfig:
    """Tests for main Config class."""

    def test_from_dict(self, temp_playbook_dir):
        """Test creating Config from dictionary."""
        data = {
            "mqtt": {"host": "localhost"},
            "worker": {
                "group_name": "test",
                "playbook_directory": str(temp_playbook_dir),
            },
            "log_level": "DEBUG",
        }
        config = Config.from_dict(data)

        assert config.mqtt.host == "localhost"
        assert config.worker.group_name == "test"
        assert config.log_level == "DEBUG"

    def test_from_dict_default_log_level(self, temp_playbook_dir):
        """Test default log level is INFO."""
        data = {
            "mqtt": {"host": "localhost"},
            "worker": {
                "group_name": "test",
                "playbook_directory": str(temp_playbook_dir),
            },
        }
        config = Config.from_dict(data)
        assert config.log_level == "INFO"

    def test_from_dict_missing_mqtt(self, temp_playbook_dir):
        """Test that missing mqtt section raises ValueError."""
        data = {
            "worker": {
                "group_name": "test",
                "playbook_directory": str(temp_playbook_dir),
            },
        }
        with pytest.raises(ValueError, match="missing required section: mqtt"):
            Config.from_dict(data)

    def test_from_dict_missing_worker(self):
        """Test that missing worker section raises ValueError."""
        data = {"mqtt": {"host": "localhost"}}
        with pytest.raises(ValueError, match="missing required section: worker"):
            Config.from_dict(data)

    def test_load_from_file(self, temp_playbook_dir):
        """Test loading configuration from YAML file."""
        config_content = f"""
mqtt:
  host: mqtt.test.com
  port: 1883

worker:
  group_name: testgroup
  playbook_directory: {temp_playbook_dir}

log_level: WARNING
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config_content)
            config_path = f.name

        try:
            config = Config.load(config_path)
            assert config.mqtt.host == "mqtt.test.com"
            assert config.worker.group_name == "testgroup"
            assert config.log_level == "WARNING"
        finally:
            os.unlink(config_path)

    def test_load_with_env_vars(self, temp_playbook_dir, monkeypatch):
        """Test loading configuration with environment variable expansion."""
        monkeypatch.setenv("MQTT_HOST", "mqtt.env.com")
        monkeypatch.setenv("MQTT_PASSWORD", "envpassword")

        config_content = f"""
mqtt:
  host: "${{MQTT_HOST}}"
  password: "${{MQTT_PASSWORD}}"

worker:
  group_name: test
  playbook_directory: {temp_playbook_dir}
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(config_content)
            config_path = f.name

        try:
            config = Config.load(config_path)
            assert config.mqtt.host == "mqtt.env.com"
            assert config.mqtt.password == "envpassword"
        finally:
            os.unlink(config_path)

    def test_load_file_not_found(self):
        """Test loading from non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            Config.load("/nonexistent/config.yaml")

    def test_load_empty_file(self):
        """Test loading empty file raises ValueError."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("")
            config_path = f.name

        try:
            with pytest.raises(ValueError, match="empty"):
                Config.load(config_path)
        finally:
            os.unlink(config_path)

    def test_validate_invalid_log_level(self, temp_playbook_dir):
        """Test validation fails for invalid log level."""
        config = Config(
            mqtt=MQTTConfig(host="localhost"),
            worker=WorkerConfig(
                group_name="test",
                playbook_directory=str(temp_playbook_dir),
            ),
            log_level="INVALID",
        )
        with pytest.raises(ValueError, match="Invalid log level"):
            config.validate()

    def test_validate_valid_log_levels(self, temp_playbook_dir):
        """Test validation passes for all valid log levels."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = Config(
                mqtt=MQTTConfig(host="localhost"),
                worker=WorkerConfig(
                    group_name="test",
                    playbook_directory=str(temp_playbook_dir),
                ),
                log_level=level,
            )
            config.validate()  # Should not raise
