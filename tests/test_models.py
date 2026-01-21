"""Tests for ansible_worker.models."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ansible_worker.models import Task, TaskRequest, TaskState, TaskStatus


class TestTaskRequest:
    """Tests for TaskRequest dataclass."""

    def test_from_dict_minimal(self):
        """Test creating TaskRequest with only required fields."""
        data = {"task_id": "abc123", "playbook": "site.yml", "inventory": "production"}
        request = TaskRequest.from_dict(data)

        assert request.task_id == "abc123"
        assert request.playbook == "site.yml"
        assert request.inventory == "production"
        assert request.extra_vars == {}
        assert request.limit is None
        assert request.tags == []
        assert request.skip_tags == []
        assert request.verbosity == 0
        assert request.check_mode is False
        assert request.diff_mode is False
        assert request.forks == 5
        assert request.timeout is None

    def test_from_dict_full(self):
        """Test creating TaskRequest with all fields."""
        data = {
            "task_id": "task-456",
            "playbook": "deploy.yml",
            "inventory": "staging",
            "extra_vars": {"version": "2.0"},
            "limit": "webservers",
            "tags": ["deploy", "config"],
            "skip_tags": ["slow"],
            "verbosity": 2,
            "check_mode": True,
            "diff_mode": True,
            "forks": 10,
            "timeout": 1800,
        }
        request = TaskRequest.from_dict(data)

        assert request.task_id == "task-456"
        assert request.playbook == "deploy.yml"
        assert request.inventory == "staging"
        assert request.extra_vars == {"version": "2.0"}
        assert request.limit == "webservers"
        assert request.tags == ["deploy", "config"]
        assert request.skip_tags == ["slow"]
        assert request.verbosity == 2
        assert request.check_mode is True
        assert request.diff_mode is True
        assert request.forks == 10
        assert request.timeout == 1800

    def test_from_dict_missing_task_id(self):
        """Test that missing task_id raises ValueError."""
        data = {"playbook": "site.yml", "inventory": "production"}
        with pytest.raises(ValueError, match="Missing required field: task_id"):
            TaskRequest.from_dict(data)

    def test_from_dict_missing_playbook(self):
        """Test that missing playbook raises ValueError."""
        data = {"task_id": "abc123", "inventory": "production"}
        with pytest.raises(ValueError, match="Missing required field: playbook"):
            TaskRequest.from_dict(data)

    def test_from_dict_missing_inventory(self):
        """Test that missing inventory raises ValueError."""
        data = {"task_id": "abc123", "playbook": "site.yml"}
        with pytest.raises(ValueError, match="Missing required field: inventory"):
            TaskRequest.from_dict(data)


class TestTaskStatus:
    """Tests for TaskStatus dataclass."""

    def test_duration_seconds_not_started(self):
        """Test duration is None when task not started."""
        status = TaskStatus(
            task_id="abc123",
            state=TaskState.QUEUED,
            created_at=datetime.now(timezone.utc),
        )
        assert status.duration_seconds is None

    def test_duration_seconds_running(self):
        """Test duration calculation for running task."""
        now = datetime.now(timezone.utc)
        status = TaskStatus(
            task_id="abc123",
            state=TaskState.RUNNING,
            created_at=now,
            started_at=now,
        )
        assert status.duration_seconds is not None
        assert status.duration_seconds >= 0

    def test_duration_seconds_completed(self):
        """Test duration calculation for completed task."""
        created = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        started = datetime(2024, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
        completed = datetime(2024, 1, 1, 10, 1, 5, tzinfo=timezone.utc)

        status = TaskStatus(
            task_id="abc123",
            state=TaskState.SUCCESS,
            created_at=created,
            started_at=started,
            completed_at=completed,
        )
        assert status.duration_seconds == 60.0

    def test_to_dict(self):
        """Test serialization to dictionary."""
        created = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        started = datetime(2024, 1, 1, 10, 0, 5, tzinfo=timezone.utc)

        status = TaskStatus(
            task_id="abc123def456",
            state=TaskState.RUNNING,
            created_at=created,
            started_at=started,
            tasks_total=10,
            tasks_ok=5,
            tasks_changed=2,
            tasks_failed=0,
            tasks_skipped=1,
            tasks_unreachable=0,
        )

        result = status.to_dict()

        assert result["task_id"] == "abc123def456"
        assert result["state"] == "running"
        assert result["created_at"] == "2024-01-01T10:00:00.000Z"
        assert result["started_at"] == "2024-01-01T10:00:05.000Z"
        assert result["completed_at"] is None
        assert result["tasks_total"] == 10
        assert result["tasks_ok"] == 5
        assert result["tasks_changed"] == 2
        assert result["tasks_failed"] == 0
        assert result["tasks_skipped"] == 1
        assert result["tasks_unreachable"] == 0
        assert result["return_code"] is None
        assert result["error_message"] is None


class TestTask:
    """Tests for Task dataclass."""

    def test_create(self):
        """Test creating a task from a request."""
        request = TaskRequest(
            task_id="my-task-id",
            playbook="site.yml",
            inventory="production",
        )
        task = Task.create(request)

        assert task.task_id == "my-task-id"
        assert task.request is request
        assert task.status.task_id == "my-task-id"
        assert task.status.state == TaskState.QUEUED
        assert task.status.created_at is not None
        assert task.status.started_at is None
        assert task.status.completed_at is None

    def test_create_preserves_task_id(self):
        """Test that task ID from request is used."""
        request1 = TaskRequest(
            task_id="task-001",
            playbook="site.yml",
            inventory="production",
        )
        request2 = TaskRequest(
            task_id="task-002",
            playbook="site.yml",
            inventory="production",
        )
        task1 = Task.create(request1)
        task2 = Task.create(request2)

        assert task1.task_id == "task-001"
        assert task2.task_id == "task-002"


class TestTaskState:
    """Tests for TaskState enum."""

    def test_all_states(self):
        """Test all expected states exist."""
        assert TaskState.QUEUED.value == "queued"
        assert TaskState.RUNNING.value == "running"
        assert TaskState.SUCCESS.value == "success"
        assert TaskState.FAILED.value == "failed"
        assert TaskState.CANCELLED.value == "cancelled"
        assert TaskState.TIMEOUT.value == "timeout"
