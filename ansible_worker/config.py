"""Configuration loading and validation for Ansible MQTT Worker."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def expand_env_vars(value: Any) -> Any:
    """Recursively expand environment variables in configuration values."""
    if isinstance(value, str):
        def replace_env_var(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is None:
                raise ValueError(f"Environment variable not set: {var_name}")
            return env_value

        return ENV_VAR_PATTERN.sub(replace_env_var, value)
    elif isinstance(value, dict):
        return {k: expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    return value


@dataclass
class MQTTConfig:
    """MQTT broker configuration."""

    host: str
    port: int = 1883
    username: str | None = None
    password: str | None = None
    keepalive: int = 60
    tls_enabled: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MQTTConfig":
        """Create MQTTConfig from dictionary."""
        if "host" not in data:
            raise ValueError("MQTT configuration missing required field: host")

        return cls(
            host=data["host"],
            port=data.get("port", 1883),
            username=data.get("username"),
            password=data.get("password"),
            keepalive=data.get("keepalive", 60),
            tls_enabled=data.get("tls_enabled", False),
        )


@dataclass
class WorkerConfig:
    """Worker configuration."""

    group_name: str
    playbook_directory: str
    topic_prefix: str = "ansible"
    max_queue_size: int = 100
    task_timeout: int = 3600

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkerConfig":
        """Create WorkerConfig from dictionary."""
        if "group_name" not in data:
            raise ValueError("Worker configuration missing required field: group_name")
        if "playbook_directory" not in data:
            raise ValueError("Worker configuration missing required field: playbook_directory")

        return cls(
            group_name=data["group_name"],
            playbook_directory=data["playbook_directory"],
            topic_prefix=data.get("topic_prefix", "ansible"),
            max_queue_size=data.get("max_queue_size", 100),
            task_timeout=data.get("task_timeout", 3600),
        )

    def validate(self) -> None:
        """Validate the worker configuration."""
        playbook_path = Path(self.playbook_directory)
        if not playbook_path.exists():
            raise ValueError(f"Playbook directory does not exist: {self.playbook_directory}")
        if not playbook_path.is_dir():
            raise ValueError(f"Playbook path is not a directory: {self.playbook_directory}")


@dataclass
class Config:
    """Main application configuration."""

    mqtt: MQTTConfig
    worker: WorkerConfig
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        """Create Config from dictionary."""
        if "mqtt" not in data:
            raise ValueError("Configuration missing required section: mqtt")
        if "worker" not in data:
            raise ValueError("Configuration missing required section: worker")

        return cls(
            mqtt=MQTTConfig.from_dict(data["mqtt"]),
            worker=WorkerConfig.from_dict(data["worker"]),
            log_level=data.get("log_level", "INFO"),
        )

    @classmethod
    def load(cls, config_path: str | Path) -> "Config":
        """Load configuration from YAML file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path) as f:
            raw_config = yaml.safe_load(f)

        if raw_config is None:
            raise ValueError("Configuration file is empty")

        expanded_config = expand_env_vars(raw_config)
        return cls.from_dict(expanded_config)

    def validate(self) -> None:
        """Validate the entire configuration."""
        self.worker.validate()

        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(f"Invalid log level: {self.log_level}")
