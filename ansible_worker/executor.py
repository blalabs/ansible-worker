"""Ansible playbook executor using ansible-runner."""

import logging
import re
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

# Pattern for valid Ansible host patterns (hostnames, IPs, groups, patterns)
# Allows: alphanumeric, dots, hyphens, underscores, colons, brackets, wildcards, commas, ampersands, exclamation
HOST_PATTERN_RE = re.compile(r'^[a-zA-Z0-9_.:\-\[\]*,&!~]+$')

# Pattern for valid extra_vars keys (Ansible variable names)
VALID_VAR_KEY_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


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

    def _validate_path_within_directory(self, path: Path, base_directory: Path) -> bool:
        """Validate that a path is within the base directory.

        Args:
            path: The path to validate.
            base_directory: The base directory the path must be within.

        Returns:
            True if the path is within the base directory, False otherwise.
        """
        try:
            resolved_path = path.resolve()
            resolved_base = base_directory.resolve()
            return resolved_path.is_relative_to(resolved_base)
        except (ValueError, OSError):
            return False

    def _validate_project_dir(self, task: Task) -> str | None:
        """Validate and return the project directory.

        Args:
            task: The task containing the project_dir request.

        Returns:
            The validated project directory path, or None if validation fails.
        """
        base_dir = Path(self._config.playbook_directory)

        if task.request.project_dir is None:
            return str(base_dir)

        project_path = Path(task.request.project_dir)

        # If relative, resolve against base directory
        if not project_path.is_absolute():
            project_path = base_dir / project_path

        if not self._validate_path_within_directory(project_path, base_dir):
            self._mark_failed(task, "Invalid project_dir: path traversal detected")
            return None

        if not project_path.exists():
            self._mark_failed(task, "Invalid project_dir: directory does not exist")
            return None

        if not project_path.is_dir():
            self._mark_failed(task, "Invalid project_dir: not a directory")
            return None

        return str(project_path)

    def _validate_playbook_path(self, task: Task, project_dir: str) -> Path | None:
        """Validate and return the playbook path.

        Args:
            task: The task containing the playbook request.
            project_dir: The validated project directory.

        Returns:
            The validated playbook path, or None if validation fails.
        """
        base_dir = Path(project_dir)
        playbook_path = base_dir / task.request.playbook

        if not self._validate_path_within_directory(playbook_path, base_dir):
            self._mark_failed(task, "Invalid playbook path: path traversal detected")
            return None

        if not playbook_path.exists():
            self._mark_failed(task, f"Playbook not found: {task.request.playbook}")
            return None

        return playbook_path

    def _validate_inventory(self, task: Task, project_dir: str) -> str | None:
        """Validate and return the inventory specification.

        Args:
            task: The task containing the inventory request.
            project_dir: The validated project directory.

        Returns:
            The validated inventory specification, or None if validation fails.
        """
        inventory = task.request.inventory

        # Check if it looks like a host pattern (not a file path)
        # Host patterns don't contain path separators and match the pattern
        if HOST_PATTERN_RE.match(inventory) and '/' not in inventory and '\\' not in inventory:
            return inventory

        # Treat as a file path - validate it's within project directory
        base_dir = Path(project_dir)
        inventory_path = base_dir / inventory

        if not self._validate_path_within_directory(inventory_path, base_dir):
            self._mark_failed(task, "Invalid inventory: path traversal detected")
            return None

        # Note: We don't require the file to exist here as ansible-runner will handle that
        return str(inventory_path)

    def _validate_extra_vars(self, task: Task) -> dict[str, Any] | None:
        """Validate extra_vars to prevent injection attacks.

        Args:
            task: The task containing extra_vars.

        Returns:
            The validated extra_vars dict, or None if validation fails.
        """
        extra_vars = task.request.extra_vars

        if not extra_vars:
            return {}

        # Validate all keys are valid Ansible variable names
        for key in extra_vars.keys():
            if not isinstance(key, str) or not VALID_VAR_KEY_RE.match(key):
                self._mark_failed(task, f"Invalid extra_vars key: {key!r}")
                return None

        # Validate values are safe types (no code objects, etc.)
        if not self._validate_extra_vars_values(extra_vars):
            self._mark_failed(task, "Invalid extra_vars: contains unsafe value types")
            return None

        return extra_vars

    def _validate_extra_vars_values(self, value: Any, depth: int = 0) -> bool:
        """Recursively validate extra_vars values are safe types.

        Args:
            value: The value to validate.
            depth: Current recursion depth (to prevent stack overflow).

        Returns:
            True if the value is safe, False otherwise.
        """
        # Prevent deeply nested structures (potential DoS)
        if depth > 10:
            return False

        if value is None:
            return True
        if isinstance(value, (str, int, float, bool)):
            return True
        if isinstance(value, list):
            return all(self._validate_extra_vars_values(item, depth + 1) for item in value)
        if isinstance(value, dict):
            return all(
                isinstance(k, str) and self._validate_extra_vars_values(v, depth + 1)
                for k, v in value.items()
            )
        # Reject any other types (functions, objects, etc.)
        return False

    def _execute_task(self, task: Task) -> None:
        """Execute a single task."""
        logger.info(f"Starting task {task.task_id}: {task.request.playbook}")

        # Validate project_dir (protects against path traversal)
        project_dir = self._validate_project_dir(task)
        if project_dir is None:
            return

        # Run git pull if requested (after project_dir validation)
        if task.request.git_pull:
            if not self._git_pull(task):
                return

        # Validate playbook path (protects against path traversal)
        playbook_path = self._validate_playbook_path(task, project_dir)
        if playbook_path is None:
            return

        # Validate inventory (protects against arbitrary file access)
        inventory = self._validate_inventory(task, project_dir)
        if inventory is None:
            return

        # Validate extra_vars (protects against injection)
        extra_vars = self._validate_extra_vars(task)
        if extra_vars is None:
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
                    project_dir=project_dir,
                    playbook=str(playbook_path),
                    inventory=inventory,
                    extravars=extra_vars or None,
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

        # Extract custom stats set via ansible.builtin.set_stats
        custom = stats.get("custom", {})
        if custom and isinstance(custom, dict):
            task.status.output.update(custom)

    def _mark_failed(self, task: Task, error_message: str) -> None:
        """Mark a task as failed."""
        task.status.state = TaskState.FAILED
        task.status.error_message = error_message
        task.status.completed_at = datetime.now(timezone.utc)
        if task.status.started_at is None:
            task.status.started_at = task.status.completed_at
        self._on_status_update(task.status)
