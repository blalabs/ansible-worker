"""Tests for ansible_worker.task_queue."""

from __future__ import annotations

import threading
import time

import pytest

from ansible_worker.models import Task, TaskRequest
from ansible_worker.task_queue import QueueFullError, TaskQueue


_task_counter = 0


def create_task(playbook: str = "test.yml") -> Task:
    """Helper to create a task."""
    global _task_counter
    _task_counter += 1
    request = TaskRequest(
        task_id=f"test-task-{_task_counter}",
        playbook=playbook,
        inventory="test",
    )
    return Task.create(request)


class TestTaskQueue:
    """Tests for TaskQueue."""

    def test_put_and_get(self):
        """Test basic put and get operations."""
        queue = TaskQueue(max_size=10)
        task = create_task()

        queue.put(task)
        assert queue.size == 1

        retrieved = queue.get(timeout=1.0)
        assert retrieved is task
        assert queue.size == 0

    def test_fifo_order(self):
        """Test that tasks are retrieved in FIFO order."""
        queue = TaskQueue(max_size=10)
        task1 = create_task("first.yml")
        task2 = create_task("second.yml")
        task3 = create_task("third.yml")

        queue.put(task1)
        queue.put(task2)
        queue.put(task3)

        assert queue.get(timeout=1.0).request.playbook == "first.yml"
        assert queue.get(timeout=1.0).request.playbook == "second.yml"
        assert queue.get(timeout=1.0).request.playbook == "third.yml"

    def test_queue_full_error(self):
        """Test that putting to a full queue raises QueueFullError."""
        queue = TaskQueue(max_size=2)
        queue.put(create_task())
        queue.put(create_task())

        with pytest.raises(QueueFullError, match="Queue is full"):
            queue.put(create_task())

    def test_is_full(self):
        """Test is_full property."""
        queue = TaskQueue(max_size=2)
        assert queue.is_full is False

        queue.put(create_task())
        assert queue.is_full is False

        queue.put(create_task())
        assert queue.is_full is True

    def test_size(self):
        """Test size property."""
        queue = TaskQueue(max_size=10)
        assert queue.size == 0

        queue.put(create_task())
        assert queue.size == 1

        queue.put(create_task())
        assert queue.size == 2

        queue.get(timeout=1.0)
        assert queue.size == 1

    def test_get_timeout(self):
        """Test that get returns None on timeout."""
        queue = TaskQueue(max_size=10)
        start = time.time()
        result = queue.get(timeout=0.1)
        elapsed = time.time() - start

        assert result is None
        assert elapsed >= 0.1
        assert elapsed < 0.5  # Some margin for test overhead

    def test_get_blocks_until_item(self):
        """Test that get blocks until an item is available."""
        queue = TaskQueue(max_size=10)
        task = create_task()
        result = []

        def producer():
            time.sleep(0.1)
            queue.put(task)

        def consumer():
            result.append(queue.get(timeout=2.0))

        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)

        consumer_thread.start()
        producer_thread.start()

        producer_thread.join()
        consumer_thread.join()

        assert len(result) == 1
        assert result[0] is task

    def test_shutdown(self):
        """Test that shutdown unblocks waiting get calls."""
        queue = TaskQueue(max_size=10)
        result = []

        def consumer():
            result.append(queue.get(timeout=10.0))

        consumer_thread = threading.Thread(target=consumer)
        consumer_thread.start()

        time.sleep(0.1)  # Let consumer start waiting
        queue.shutdown()
        consumer_thread.join(timeout=1.0)

        assert not consumer_thread.is_alive()
        assert result == [None]

    def test_clear(self):
        """Test clearing the queue."""
        queue = TaskQueue(max_size=10)
        task1 = create_task("one.yml")
        task2 = create_task("two.yml")

        queue.put(task1)
        queue.put(task2)
        assert queue.size == 2

        cleared = queue.clear()
        assert queue.size == 0
        assert len(cleared) == 2
        assert task1 in cleared
        assert task2 in cleared

    def test_clear_empty_queue(self):
        """Test clearing an empty queue."""
        queue = TaskQueue(max_size=10)
        cleared = queue.clear()
        assert cleared == []

    def test_thread_safety(self):
        """Test that the queue is thread-safe."""
        queue = TaskQueue(max_size=100)
        num_producers = 5
        num_items_per_producer = 20
        results = []
        results_lock = threading.Lock()

        def producer(producer_id: int):
            for i in range(num_items_per_producer):
                task = create_task(f"task_{producer_id}_{i}.yml")
                queue.put(task)

        def consumer():
            while True:
                task = queue.get(timeout=0.5)
                if task is None:
                    break
                with results_lock:
                    results.append(task)

        # Start producers
        producers = [
            threading.Thread(target=producer, args=(i,))
            for i in range(num_producers)
        ]
        for p in producers:
            p.start()

        # Start consumers
        consumers = [threading.Thread(target=consumer) for _ in range(3)]
        for c in consumers:
            c.start()

        # Wait for producers to finish
        for p in producers:
            p.join()

        # Wait a bit for consumers to drain the queue
        time.sleep(0.5)

        # Shutdown queue to stop consumers
        queue.shutdown()
        for c in consumers:
            c.join()

        expected_count = num_producers * num_items_per_producer
        assert len(results) == expected_count
