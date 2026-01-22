"""Ansible playbook executor using ansible-runner."""

import logging
import subprocess
import tempfile
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ansible_runner

from ansible_worker.config import WorkerConfig
from ansible_worker.models import Task, TaskState, TaskStatus
from ansible_worker.task_queue import TaskQueue

logger = logging.getLogger(__name__)


class Executor:
    """Executes Ansible playbooks from the task queue."""

    def __init__(
        self,
        config: WorkerConfig,
        task_queue: TaskQueue,
        on_status_update: Callable[[TaskStatus], None],
    ) -> None:
        """Initialize the executor.

        Args:
            config: Worker configuration.
            task_queue: Queue to pull tasks from.
            on_status_update: Callback for status updates.
        """
        self._config = config
        self._task_queue = task_queue
        self._on_status_update = on_status_update
        self._thread: threading.Thread | None = None
        self._shutdown = threading.Event()
        self._current_task: Task | None = None
        self._current_runner: ansible_runner.Runner | None = None

    def start(self) -> None:
        """Start the executor thread."""
        self._shutdown.clear()
        self._thread = threading.Thread(target=self._run, name="executor", daemon=True)
        self._thread.start()
        logger.info("Executor thread started")

    def stop(self) -> None:
        """Stop the executor thread."""
        self._shutdown.set()
        self._task_queue.shutdown()

        if self._current_runner:
            logger.info("Cancelling current task")
            self._current_runner.cancel()

        if self._thread:
            self._thread.join(timeout=10.0)
            if self._thread.is_alive():
                logger.warning("Executor thread did not stop cleanly")

    def _run(self) -> None:
        """Main executor loop."""
        while not self._shutdown.is_set():
            task = self._task_queue.get(timeout=1.0)
            if task is None:
                continue

            self._current_task = task
            try:
                self._execute_task(task)
            except Exception as e:
                logger.exception(f"Unexpected error executing task {task.task_id}: {e}")
                self._mark_failed(task, str(e))
            finally:
                self._current_task = None
                self._current_runner = None

    def _execute_task(self, task: Task) -> None:
        """Execute a single task."""
        logger.info(f"Starting task {task.task_id}: {task.request.playbook}")

        # Run git pull if requested
        if task.request.git_pull:
            if not self._git_pull(task):
                return

        playbook_path = Path(self._config.playbook_directory) / task.request.playbook
        if not playbook_path.exists():
            self._mark_failed(task, f"Playbook not found: {task.request.playbook}")
            return

        task.status.state = TaskState.RUNNING
        task.status.started_at = datetime.now(timezone.utc)
        self._on_status_update(task.status)

        timeout = task.request.timeout or self._config.task_timeout

        cmdline_args = self._build_cmdline_args(task)

        def event_handler(event: dict[str, Any]) -> bool:
            """Handle ansible-runner events."""
            if self._shutdown.is_set():
                return False
            self._process_event(task, event)
            return True

        def status_handler(status: dict[str, Any], runner_config: Any) -> bool:
            """Handle ansible-runner status updates."""
            if self._shutdown.is_set():
                return False
            return True

        try:
            with tempfile.TemporaryDirectory(prefix="ansible_worker_") as temp_dir:
                runner = ansible_runner.run(
                    private_data_dir=temp_dir,
                    project_dir=task.request.project_dir or self._config.playbook_directory,
                    playbook=str(playbook_path),
                    inventory=task.request.inventory,
                    extravars=task.request.extra_vars or None,
                    cmdline=cmdline_args if cmdline_args else None,
                    event_handler=event_handler,
                    status_handler=status_handler,
                    timeout=timeout,
                    quiet=True,
                )

                self._current_runner = runner

                task.status.return_code = runner.rc
                task.status.completed_at = datetime.now(timezone.utc)

                if runner.status == "canceled":
                    task.status.state = TaskState.CANCELLED
                    logger.info(f"Task {task.task_id} cancelled")
                elif runner.status == "timeout":
                    task.status.state = TaskState.TIMEOUT
                    logger.warning(f"Task {task.task_id} timed out")
                elif runner.rc == 0:
                    task.status.state = TaskState.SUCCESS
                    logger.info(f"Task {task.task_id} completed successfully")
                else:
                    task.status.state = TaskState.FAILED
                    task.status.error_message = f"Playbook failed with return code {runner.rc}"
                    logger.error(f"Task {task.task_id} failed with rc={runner.rc}")

                self._on_status_update(task.status)

        except Exception as e:
            logger.exception(f"Error running playbook: {e}")
            self._mark_failed(task, str(e))

    def _build_cmdline_args(self, task: Task) -> str:
        """Build command line arguments for ansible-playbook."""
        args = []

        if task.request.limit:
            args.append(f"--limit={task.request.limit}")

        if task.request.tags:
            args.append(f"--tags={','.join(task.request.tags)}")

        if task.request.skip_tags:
            args.append(f"--skip-tags={','.join(task.request.skip_tags)}")

        if task.request.verbosity > 0:
            args.append("-" + "v" * task.request.verbosity)

        if task.request.check_mode:
            args.append("--check")

        if task.request.diff_mode:
            args.append("--diff")

        if task.request.forks != 5:
            args.append(f"--forks={task.request.forks}")

        return " ".join(args)

    def _git_pull(self, task: Task) -> bool:
        """Run git pull in the playbook directory.

        Args:
            task: The task requesting the git pull.

        Returns:
            True if git pull succeeded, False otherwise.
        """
        logger.info(f"Task {task.task_id}: Running git pull in {self._config.playbook_directory}")

        try:
            result = subprocess.run(
                ["git", "pull"],
                cwd=self._config.playbook_directory,
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                error_msg = f"git pull failed: {result.stderr.strip() or result.stdout.strip()}"
                logger.error(f"Task {task.task_id}: {error_msg}")
                self._mark_failed(task, error_msg)
                return False

            logger.info(f"Task {task.task_id}: git pull successful")
            return True

        except subprocess.TimeoutExpired:
            error_msg = "git pull timed out after 60 seconds"
            logger.error(f"Task {task.task_id}: {error_msg}")
            self._mark_failed(task, error_msg)
            return False
        except FileNotFoundError:
            error_msg = "git command not found"
            logger.error(f"Task {task.task_id}: {error_msg}")
            self._mark_failed(task, error_msg)
            return False
        except Exception as e:
            error_msg = f"git pull error: {e}"
            logger.error(f"Task {task.task_id}: {error_msg}")
            self._mark_failed(task, error_msg)
            return False

    def _process_event(self, task: Task, event: dict[str, Any]) -> None:
        """Process an ansible-runner event."""
        event_type = event.get("event", "")

        if event_type == "playbook_on_stats":
            stats = event.get("event_data", {})
            self._update_stats_from_event(task, stats)
            self._on_status_update(task.status)

        elif event_type == "runner_on_ok":
            task.status.tasks_ok += 1
            event_data = event.get("event_data", {})
            if event_data.get("res", {}).get("changed", False):
                task.status.tasks_changed += 1

        elif event_type == "runner_on_failed":
            task.status.tasks_failed += 1

        elif event_type == "runner_on_skipped":
            task.status.tasks_skipped += 1

        elif event_type == "runner_on_unreachable":
            task.status.tasks_unreachable += 1

        elif event_type == "playbook_on_task_start":
            task.status.tasks_total += 1

    def _update_stats_from_event(self, task: Task, stats: dict[str, Any]) -> None:
        """Update task status from playbook stats event."""
        ok_hosts = stats.get("ok", {})
        changed_hosts = stats.get("changed", {})
        failed_hosts = stats.get("failures", {})
        skipped_hosts = stats.get("skipped", {})
        unreachable_hosts = stats.get("dark", {})

        task.status.tasks_ok = sum(ok_hosts.values()) if ok_hosts else 0
        task.status.tasks_changed = sum(changed_hosts.values()) if changed_hosts else 0
        task.status.tasks_failed = sum(failed_hosts.values()) if failed_hosts else 0
        task.status.tasks_skipped = sum(skipped_hosts.values()) if skipped_hosts else 0
        task.status.tasks_unreachable = sum(unreachable_hosts.values()) if unreachable_hosts else 0

    def _mark_failed(self, task: Task, error_message: str) -> None:
        """Mark a task as failed."""
        task.status.state = TaskState.FAILED
        task.status.error_message = error_message
        task.status.completed_at = datetime.now(timezone.utc)
        if task.status.started_at is None:
            task.status.started_at = task.status.completed_at
        self._on_status_update(task.status)
