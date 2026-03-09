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
        assert call_kwargs["playbook"] == str(playbook_path)
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


class TestExecutorGitPull:
    """Tests for git pull functionality."""

    @patch("ansible_worker.executor.subprocess.run")
    @patch("ansible_worker.executor.ansible_runner.run")
    def test_git_pull_success(
        self,
        mock_ansible_run: MagicMock,
        mock_subprocess_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test successful git pull before playbook execution."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock successful git pull
        mock_subprocess_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        # Mock successful ansible run
        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_ansible_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-git-pull-001",
            playbook="site.yml",
            inventory="test",
            git_pull=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        # Verify git pull was called
        mock_subprocess_run.assert_called_once()
        call_args = mock_subprocess_run.call_args
        assert call_args[0][0] == ["git", "pull"]
        assert call_args[1]["cwd"] == str(tmp_path)

        # Verify ansible-runner was called
        mock_ansible_run.assert_called_once()

        # Verify success status
        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS

    @patch("ansible_worker.executor.subprocess.run")
    @patch("ansible_worker.executor.ansible_runner.run")
    def test_git_pull_failure(
        self,
        mock_ansible_run: MagicMock,
        mock_subprocess_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test failed git pull marks task as failed."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock failed git pull
        mock_subprocess_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="error: cannot pull with rebase",
        )

        request = TaskRequest(
            task_id="test-git-pull-002",
            playbook="site.yml",
            inventory="test",
            git_pull=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        # Verify git pull was called
        mock_subprocess_run.assert_called_once()

        # Verify ansible-runner was NOT called
        mock_ansible_run.assert_not_called()

        # Verify failed status
        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "git pull failed" in final_status.error_message

    @patch("ansible_worker.executor.subprocess.run")
    @patch("ansible_worker.executor.ansible_runner.run")
    def test_git_pull_timeout(
        self,
        mock_ansible_run: MagicMock,
        mock_subprocess_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test git pull timeout marks task as failed."""
        import subprocess

        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock git pull timeout
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd="git pull", timeout=60)

        request = TaskRequest(
            task_id="test-git-pull-003",
            playbook="site.yml",
            inventory="test",
            git_pull=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        # Verify ansible-runner was NOT called
        mock_ansible_run.assert_not_called()

        # Verify failed status
        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "timed out" in final_status.error_message

    @patch("ansible_worker.executor.subprocess.run")
    @patch("ansible_worker.executor.ansible_runner.run")
    def test_git_command_not_found(
        self,
        mock_ansible_run: MagicMock,
        mock_subprocess_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test git command not found marks task as failed."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock git not found
        mock_subprocess_run.side_effect = FileNotFoundError()

        request = TaskRequest(
            task_id="test-git-pull-004",
            playbook="site.yml",
            inventory="test",
            git_pull=True,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        # Verify ansible-runner was NOT called
        mock_ansible_run.assert_not_called()

        # Verify failed status
        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "git command not found" in final_status.error_message

    @patch("ansible_worker.executor.subprocess.run")
    @patch("ansible_worker.executor.ansible_runner.run")
    def test_git_pull_disabled_by_default(
        self,
        mock_ansible_run: MagicMock,
        mock_subprocess_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that git pull is not executed when git_pull=False (default)."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        # Mock successful ansible run
        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_ansible_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-git-pull-005",
            playbook="site.yml",
            inventory="test",
            # git_pull defaults to False
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        # Verify git pull was NOT called
        mock_subprocess_run.assert_not_called()

        # Verify ansible-runner was called
        mock_ansible_run.assert_called_once()

        # Verify success status
        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS


class TestExecutorSecurityValidation:
    """Tests for security validation in executor."""

    def test_playbook_path_traversal_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that path traversal in playbook path is blocked."""
        # Create a playbook outside the allowed directory
        (tmp_path / "allowed").mkdir()
        (tmp_path / "secret.yml").write_text("---\n- hosts: all\n")

        # Reconfigure executor with subdirectory as playbook_directory
        executor._config.playbook_directory = str(tmp_path / "allowed")

        request = TaskRequest(
            task_id="test-traversal-001",
            playbook="../secret.yml",
            inventory="localhost,",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "path traversal" in final_status.error_message.lower()

    def test_project_dir_traversal_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that path traversal in project_dir is blocked."""
        (tmp_path / "allowed").mkdir()
        (tmp_path / "allowed" / "site.yml").write_text("---\n- hosts: all\n")

        executor._config.playbook_directory = str(tmp_path / "allowed")

        request = TaskRequest(
            task_id="test-traversal-002",
            playbook="site.yml",
            inventory="localhost,",
            project_dir=str(tmp_path),  # Parent directory - not allowed
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "path traversal" in final_status.error_message.lower()

    def test_project_dir_relative_traversal_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that relative path traversal in project_dir is blocked."""
        (tmp_path / "allowed").mkdir()
        (tmp_path / "allowed" / "site.yml").write_text("---\n- hosts: all\n")

        executor._config.playbook_directory = str(tmp_path / "allowed")

        request = TaskRequest(
            task_id="test-traversal-003",
            playbook="site.yml",
            inventory="localhost,",
            project_dir="../",  # Relative traversal - not allowed
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "path traversal" in final_status.error_message.lower()

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_project_dir_subdirectory_allowed(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that subdirectories of playbook_directory are allowed."""
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "site.yml").write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-subdir-001",
            playbook="site.yml",
            inventory="localhost,",
            project_dir=str(tmp_path / "subdir"),
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS

    def test_inventory_path_traversal_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that inventory path traversal is blocked."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        request = TaskRequest(
            task_id="test-inventory-001",
            playbook="site.yml",
            inventory="../../../etc/passwd",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "path traversal" in final_status.error_message.lower()

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_inventory_host_pattern_allowed(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that valid host patterns are allowed as inventory."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        # Test various valid host patterns
        valid_patterns = [
            "localhost,",
            "all",
            "webservers",
            "192.168.1.1,",
            "web[1:10].example.com",
            "webservers:dbservers",
            "webservers:&staging",
            "webservers:!excluded",
        ]

        for pattern in valid_patterns:
            mock_run.reset_mock()
            status_updates.clear()

            request = TaskRequest(
                task_id=f"test-pattern-{pattern[:10]}",
                playbook="site.yml",
                inventory=pattern,
            )
            task = Task.create(request)

            task_queue.put(task)
            executor.start()
            import time
            time.sleep(0.3)
            executor.stop()

            final_status = status_updates[-1]
            assert final_status.state == TaskState.SUCCESS, f"Pattern {pattern} should be allowed"

    def test_extra_vars_invalid_key_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that invalid extra_vars keys are blocked."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        request = TaskRequest(
            task_id="test-extra-vars-001",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars={"valid_key": "value", "invalid-key-with-dash": "value"},
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "invalid extra_vars key" in final_status.error_message.lower()

    def test_extra_vars_key_starting_with_number_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that extra_vars keys starting with numbers are blocked."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        request = TaskRequest(
            task_id="test-extra-vars-002",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars={"1invalid": "value"},
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "invalid extra_vars key" in final_status.error_message.lower()

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_extra_vars_valid_keys_allowed(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that valid extra_vars keys are allowed."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-extra-vars-003",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars={
                "valid_key": "value",
                "_private": "value",
                "CamelCase": "value",
                "with_numbers_123": "value",
            },
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS

    def test_extra_vars_deeply_nested_blocked(
        self,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that deeply nested extra_vars are blocked (DoS protection)."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        # Create deeply nested structure (11 levels deep)
        nested = {"level": "bottom"}
        for i in range(12):
            nested = {"nested": nested}

        request = TaskRequest(
            task_id="test-extra-vars-004",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars=nested,
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.FAILED
        assert "unsafe value" in final_status.error_message.lower()

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_extra_vars_nested_dict_allowed(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that reasonably nested extra_vars are allowed."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-extra-vars-005",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars={
                "app": {
                    "name": "myapp",
                    "version": "1.0.0",
                    "config": {
                        "debug": True,
                        "port": 8080,
                    },
                },
                "hosts": ["host1", "host2"],
            },
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS

class TestExecutorOutput:
    """Tests for task output from set_stats."""

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_custom_stats_populate_output(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that custom stats from set_stats are included in task output."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        def capture_event_handler(mock_runner, event_handler):
            """Simulate ansible-runner events with custom stats."""
            event_handler({
                "event": "playbook_on_task_start",
                "event_data": {"name": "create vm"},
            })
            event_handler({
                "event": "runner_on_ok",
                "event_data": {"res": {"changed": True}},
            })
            event_handler({
                "event": "playbook_on_stats",
                "event_data": {
                    "ok": {"localhost": 1},
                    "changed": {"localhost": 1},
                    "failures": {},
                    "skipped": {},
                    "dark": {},
                    "custom": {
                        "vm_ip": "10.0.0.50",
                        "vm_id": 105,
                    },
                },
            })

        def side_effect(**kwargs):
            event_handler = kwargs["event_handler"]
            mock_runner = MagicMock()
            mock_runner.status = "successful"
            mock_runner.rc = 0
            capture_event_handler(mock_runner, event_handler)
            return mock_runner

        mock_run.side_effect = side_effect

        request = TaskRequest(
            task_id="test-output-001",
            playbook="site.yml",
            inventory="localhost,",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS
        assert final_status.output == {"vm_ip": "10.0.0.50", "vm_id": 105}

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_empty_custom_stats_leave_output_empty(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that missing custom stats result in empty output."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        def side_effect(**kwargs):
            event_handler = kwargs["event_handler"]
            event_handler({
                "event": "playbook_on_stats",
                "event_data": {
                    "ok": {"localhost": 1},
                    "changed": {},
                    "failures": {},
                    "skipped": {},
                    "dark": {},
                },
            })
            mock_runner = MagicMock()
            mock_runner.status = "successful"
            mock_runner.rc = 0
            return mock_runner

        mock_run.side_effect = side_effect

        request = TaskRequest(
            task_id="test-output-002",
            playbook="site.yml",
            inventory="localhost,",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS
        assert final_status.output == {}

    @patch("ansible_worker.executor.ansible_runner.run")
    def test_output_included_in_status_dict(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that output is included in the serialized status dict."""
        playbook_path = tmp_path / "site.yml"
        playbook_path.write_text("---\n- hosts: all\n")

        def side_effect(**kwargs):
            event_handler = kwargs["event_handler"]
            event_handler({
                "event": "playbook_on_stats",
                "event_data": {
                    "ok": {"localhost": 1},
                    "changed": {},
                    "failures": {},
                    "skipped": {},
                    "dark": {},
                    "custom": {"server_ip": "192.168.1.100"},
                },
            })
            mock_runner = MagicMock()
            mock_runner.status = "successful"
            mock_runner.rc = 0
            return mock_runner

        mock_run.side_effect = side_effect

        request = TaskRequest(
            task_id="test-output-003",
            playbook="site.yml",
            inventory="localhost,",
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        status_dict = final_status.to_dict()
        assert status_dict["output"] == {"server_ip": "192.168.1.100"}


    @patch("ansible_worker.executor.ansible_runner.run")
    def test_empty_extra_vars_allowed(
        self,
        mock_run: MagicMock,
        executor: Executor,
        task_queue: TaskQueue,
        status_updates: list[TaskStatus],
        tmp_path: Path,
    ):
        """Test that empty extra_vars are allowed."""
        (tmp_path / "site.yml").write_text("---\n- hosts: all\n")

        mock_runner = MagicMock()
        mock_runner.status = "successful"
        mock_runner.rc = 0
        mock_run.return_value = mock_runner

        request = TaskRequest(
            task_id="test-extra-vars-006",
            playbook="site.yml",
            inventory="localhost,",
            extra_vars={},
        )
        task = Task.create(request)

        task_queue.put(task)
        executor.start()
        import time
        time.sleep(0.5)
        executor.stop()

        final_status = status_updates[-1]
        assert final_status.state == TaskState.SUCCESS