"""Thread-safe task queue for sequential task execution."""

import logging
import threading
from collections import deque

from ansible_worker.models import Task

logger = logging.getLogger(__name__)


class QueueFullError(Exception):
    """Raised when attempting to add a task to a full queue."""

    pass


class TaskQueue:
    """Thread-safe queue for managing tasks."""

    def __init__(self, max_size: int = 100) -> None:
        """Initialize the task queue.

        Args:
            max_size: Maximum number of tasks that can be queued.
        """
        self._queue: deque[Task] = deque()
        self._max_size = max_size
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._shutdown = False

    def put(self, task: Task) -> None:
        """Add a task to the queue.

        Args:
            task: The task to add.

        Raises:
            QueueFullError: If the queue is at maximum capacity.
        """
        with self._lock:
            if len(self._queue) >= self._max_size:
                raise QueueFullError(
                    f"Queue is full (max size: {self._max_size})"
                )
            self._queue.append(task)
            logger.debug(f"Task {task.task_id} added to queue (size: {len(self._queue)})")
            self._not_empty.notify()

    def get(self, timeout: float | None = None) -> Task | None:
        """Get the next task from the queue.

        Blocks until a task is available or timeout occurs.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            The next task, or None if timeout occurred or shutdown was requested.
        """
        with self._not_empty:
            while not self._queue and not self._shutdown:
                if not self._not_empty.wait(timeout):
                    return None

            if self._shutdown and not self._queue:
                return None

            if self._queue:
                task = self._queue.popleft()
                logger.debug(f"Task {task.task_id} dequeued (remaining: {len(self._queue)})")
                return task

            return None

    def shutdown(self) -> None:
        """Signal shutdown and wake up waiting threads."""
        with self._lock:
            self._shutdown = True
            self._not_empty.notify_all()

    def clear(self) -> list[Task]:
        """Clear all tasks from the queue and return them.

        Returns:
            List of tasks that were in the queue.
        """
        with self._lock:
            tasks = list(self._queue)
            self._queue.clear()
            return tasks

    @property
    def size(self) -> int:
        """Get the current number of tasks in the queue."""
        with self._lock:
            return len(self._queue)

    @property
    def is_full(self) -> bool:
        """Check if the queue is full."""
        with self._lock:
            return len(self._queue) >= self._max_size
