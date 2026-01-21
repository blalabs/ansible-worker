"""Shared pytest fixtures for ansible-worker tests."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from ansible_worker.config import Config, MQTTConfig, WorkerConfig
from ansible_worker.models import Task, TaskRequest


@pytest.fixture
def mqtt_config() -> MQTTConfig:
    """Create a test MQTT configuration."""
    return MQTTConfig(
        host="localhost",
        port=1883,
        username="test",
        password="testpass",
    )


@pytest.fixture
def temp_playbook_dir() -> Path:
    """Create a temporary playbook directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def worker_config(temp_playbook_dir: Path) -> WorkerConfig:
    """Create a test worker configuration."""
    return WorkerConfig(
        group_name="test-group",
        playbook_directory=str(temp_playbook_dir),
        topic_prefix="ansible",
        max_queue_size=10,
        task_timeout=300,
    )


@pytest.fixture
def config(mqtt_config: MQTTConfig, worker_config: WorkerConfig) -> Config:
    """Create a test application configuration."""
    return Config(
        mqtt=mqtt_config,
        worker=worker_config,
        log_level="DEBUG",
    )


@pytest.fixture
def sample_task_request() -> TaskRequest:
    """Create a sample task request."""
    return TaskRequest(
        task_id="test-task-123",
        playbook="site.yml",
        inventory="production",
        extra_vars={"app_version": "1.0.0"},
        limit="webservers",
        tags=["deploy"],
        verbosity=1,
    )


@pytest.fixture
def sample_task(sample_task_request: TaskRequest) -> Task:
    """Create a sample task."""
    return Task.create(sample_task_request)
