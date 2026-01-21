"""Tests for ansible_worker.executor."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ansible_worker.config import WorkerConfig
from ansible_worker.executor import Executor
from ansible_worker.models import Task, TaskRequest, TaskState, TaskStatus
from ansible_worker.task_queue import TaskQueue


@pytest.fixture
def worker_config(tmp_path: Path) -> WorkerConfig:
    """Create a test worker configuration."""
    return WorkerConfig(
        group_name="test",
        playbook_directory=str(tmp_path),
        max_queue_size=10,
        task_timeout=60,
    )


@pytest.fixture
def task_queue() -> TaskQueue:
    """Create a test task queue."""
    return TaskQueue(max_size=10)


@pytest.fixture
def status_updates() -> list[TaskStatus]:
    """Collect status updates."""
    return []


@pytest.fixture
def executor(
    worker_config: WorkerConfig,
    task_queue: TaskQueue,
    status_updates: list[TaskStatus],
) -> Executor:
    """Create a test executor."""
    return Executor(
        config=worker_config,
        task_queue=task_queue,
        on_status_update=lambda s: status_updates.append(s),
    )


class TestExecutor:
    """Tests for Executor."""

    def test_start_and_stop(self, executor: Executor):
        """Test starting and stopping the executor."""
        executor.start()
        assert executor._thread is not None
        assert executor._thread.is_alive()

        executor.stop()
        assert not executor._thread.is_alive()

    def test_playbook_not_found(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
    ):
        """Test handling of non-existent playbook."""
        request = TaskRequest(
            task_id="test-task-001",
            playbook="nonexistent.yml",
            inventory="test",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        # Wait for task to be processed
        time.sleep(0.5)
        executor.stop()

        # Should have status updates
        assert len(status_updates) >= 1

        # Final status should be FAILED
        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "not found" in final_status.error_message.lower()

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_successful_execution(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test successful playbook execution."""
        # Create a playbook file
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock successful runner
        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-task-002",
            playbook="site.yml",
            inventory="test",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        # Wait for task to be processed
        time.sleep(0.5)
        executor.stop()

        # Verify ansible-runner was called
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["playbook"] == "site.yml"
        assert call_kwargs["inventory"] == "test"

        # Check status progression
        states = [s.state for s in status_updates]
        assert TaskState.SUCCESS in states

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_failed_execution(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test failed playbook execution."""
        playbook_path = tmp_path / "failing.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "failed"
        mock_runner.rc = 2
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-task-003",
            playbook="failing.yml",
            inventory="test",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert final_status.return_code == 2

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_timeout_execution(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test timed out playbook execution."""
        playbook_path = tmp_path / "slow.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "timeout"
        mock_runner.rc = -1
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-task-004",
            playbook="slow.yml",
            inventory="test",
            timeout=1,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.TIMEOUT

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_cancelled_execution(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test cancelled playbook execution."""
        playbook_path = tmp_path / "cancel.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "canceled"
        mock_runner.rc = -1
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-task-005",
            playbook="cancel.yml",
            inventory="test",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.CANCELLED

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_task_id_preserved(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that task_id from request is preserved through execution."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="my-custom-task-id-123",
            playbook="site.yml",
            inventory="test",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()

        time.sleep(0.5)
        executor.stop()

        # All status updates should have the same task_id
        for status in status_updates:
            assert status.task_id == "my-custom-task-id-123"


class TestExecutorCommandLineArgs:
    """Tests for command line argument building."""

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_limit_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --limit argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-limit-001",
            playbook="site.yml",
            inventory="test",
            limit="webservers",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--limit=webservers" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_tags_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --tags argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-tags-001",
            playbook="site.yml",
            inventory="test",
            tags=["deploy", "config"],
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--tags=deploy,config" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_skip_tags_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --skip-tags argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-skip-tags-001",
            playbook="site.yml",
            inventory="test",
            skip_tags=["slow", "optional"],
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--skip-tags=slow,optional" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_verbosity_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test verbosity argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-verbosity-001",
            playbook="site.yml",
            inventory="test",
            verbosity=3,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "-vvv" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_check_mode_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --check argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-check-001",
            playbook="site.yml",
            inventory="test",
            check_mode=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--check" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_diff_mode_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --diff argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-diff-001",
            playbook="site.yml",
            inventory="test",
            diff_mode=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--diff" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_forks_arg(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test --forks argument."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-forks-001",
            playbook="site.yml",
            inventory="test",
            forks=20,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        assert "--forks=20" in call_kwargs["cmdline"]

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_default_forks_not_included(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        tmp_path: Path,
    ):
        """Test that default forks value is not included in cmdline."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-default-forks-001",
            playbook="site.yml",
            inventory="test",
            forks=5,  # Default value
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        call_kwargs = mock_run.call_args[1]
        # cmdline should be None or not contain forks when using default
        cmdline = call_kwargs.get("cmdline")
        if cmdline:
            assert "--forks" not in cmdline
