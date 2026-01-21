"""Data models for Ansible MQTT Worker."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class TaskState(Enum):
    """Possible states for a task."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


@dataclass
class TaskRequest:
    """Incoming task request from MQTT message."""

    task_id: str
    playbook: str
    inventory: str
    extra_vars: dict[str, Any] = field(default_factory=dict)
    limit: str | None = None
    tags: list[str] = field(default_factory=list)
    skip_tags: list[str] = field(default_factory=list)
    verbosity: int = 0
    check_mode: bool = False
    diff_mode: bool = False
    forks: int = 5
    timeout: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskRequest":
        """Create TaskRequest from dictionary, validating required fields."""
        if "task_id" not in data:
            raise ValueError("Missing required field: task_id")
        if "playbook" not in data:
            raise ValueError("Missing required field: playbook")
        if "inventory" not in data:
            raise ValueError("Missing required field: inventory")

        return cls(
            task_id=data["task_id"],
            playbook=data["playbook"],
            inventory=data["inventory"],
            extra_vars=data.get("extra_vars", {}),
            limit=data.get("limit"),
            tags=data.get("tags", []),
            skip_tags=data.get("skip_tags", []),
            verbosity=data.get("verbosity", 0),
            check_mode=data.get("check_mode", False),
            diff_mode=data.get("diff_mode", False),
            forks=data.get("forks", 5),
            timeout=data.get("timeout"),
        )


@dataclass
class TaskStatus:
    """Status update for a task."""

    task_id: str
    state: TaskState
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    tasks_total: int = 0
    tasks_ok: int = 0
    tasks_changed: int = 0
    tasks_failed: int = 0
    tasks_skipped: int = 0
    tasks_unreachable: int = 0
    return_code: int | None = None
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Calculate duration in seconds."""
        if self.started_at is None:
            return None
        end_time = self.completed_at or datetime.now(timezone.utc)
        return (end_time - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "created_at": self.created_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "started_at": (
                self.started_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                if self.started_at
                else None
            ),
            "completed_at": (
                self.completed_at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                if self.completed_at
                else None
            ),
            "duration_seconds": self.duration_seconds,
            "tasks_total": self.tasks_total,
            "tasks_ok": self.tasks_ok,
            "tasks_changed": self.tasks_changed,
            "tasks_failed": self.tasks_failed,
            "tasks_skipped": self.tasks_skipped,
            "tasks_unreachable": self.tasks_unreachable,
            "return_code": self.return_code,
            "error_message": self.error_message,
        }


@dataclass
class Task:
    """A task to be executed."""

    task_id: str
    request: TaskRequest
    status: TaskStatus

    @classmethod
    def create(cls, request: TaskRequest) -> "Task":
        """Create a new task from request with initial status."""
        now = datetime.now(timezone.utc)
        status = TaskStatus(
            task_id=request.task_id,
            state=TaskState.QUEUED,
            created_at=now,
        )
        return cls(task_id=request.task_id, request=request, status=status)
