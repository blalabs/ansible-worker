//! A bounded, async FIFO queue connecting the transport (producer) to the
//! [`Executor`](crate::executor::Executor) (consumer). Clones share the same
//! underlying queue, so producer and consumer can each hold a handle.

use crate::models::Task;
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, Notify};

/// Errors returned when enqueueing a task.
#[derive(Error, Debug)]
pub enum QueueError {
    /// The queue has reached its configured maximum size.
    #[error("Queue is full (max size: {0})")]
    QueueFull(usize),
    /// The queue has been shut down and is no longer accepting tasks.
    #[error("Queue is shutdown")]
    Shutdown,
}

/// Thread-safe FIFO task queue with async support
pub struct TaskQueue {
    inner: Arc<TaskQueueInner>,
}

struct TaskQueueInner {
    queue: Mutex<VecDeque<Task>>,
    max_size: usize,
    notify: Notify,
    shutdown: Mutex<bool>,
}

impl TaskQueue {
    /// Create a new task queue with the given maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: Arc::new(TaskQueueInner {
                queue: Mutex::new(VecDeque::new()),
                max_size,
                notify: Notify::new(),
                shutdown: Mutex::new(false),
            }),
        }
    }

    /// Add a task to the queue
    /// Returns an error if the queue is full
    pub async fn put(&self, task: Task) -> Result<(), QueueError> {
        let mut queue = self.inner.queue.lock().await;

        if queue.len() >= self.inner.max_size {
            return Err(QueueError::QueueFull(self.inner.max_size));
        }

        queue.push_back(task);
        self.inner.notify.notify_one();
        Ok(())
    }

    /// Get the next task from the queue, waiting if necessary
    /// Returns None if the queue is shutdown
    pub async fn get(&self) -> Option<Task> {
        loop {
            // Check shutdown first
            {
                let shutdown = self.inner.shutdown.lock().await;
                if *shutdown {
                    return None;
                }
            }

            // Try to get a task
            {
                let mut queue = self.inner.queue.lock().await;
                if let Some(task) = queue.pop_front() {
                    return Some(task);
                }
            }

            // Wait for notification
            self.inner.notify.notified().await;
        }
    }

    /// Get the next task with a timeout
    /// Returns None if timeout expires or queue is shutdown
    pub async fn get_timeout(&self, timeout: std::time::Duration) -> Option<Task> {
        tokio::select! {
            task = self.get() => task,
            _ = tokio::time::sleep(timeout) => None,
        }
    }

    /// Get the current queue length
    pub async fn len(&self) -> usize {
        self.inner.queue.lock().await.len()
    }

    /// Check if the queue is empty
    pub async fn is_empty(&self) -> bool {
        self.inner.queue.lock().await.is_empty()
    }

    /// Signal shutdown and wake any waiting consumers
    pub async fn shutdown(&self) {
        let mut shutdown = self.inner.shutdown.lock().await;
        *shutdown = true;
        self.inner.notify.notify_waiters();
    }

    /// Clear the queue and return all remaining tasks
    pub async fn clear(&self) -> Vec<Task> {
        let mut queue = self.inner.queue.lock().await;
        queue.drain(..).collect()
    }

    /// Check if the queue is shutdown
    pub async fn is_shutdown(&self) -> bool {
        *self.inner.shutdown.lock().await
    }
}

impl Clone for TaskQueue {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TaskRequest;

    fn make_task(id: &str) -> Task {
        let request = TaskRequest::from_json(
            format!(
                r#"{{"task_id": "{}", "playbook": "test.yml", "inventory": "test"}}"#,
                id
            )
            .as_bytes(),
        )
        .unwrap();
        Task::new(request)
    }

    #[tokio::test]
    async fn test_queue_put_get() {
        let queue = TaskQueue::new(10);

        queue.put(make_task("task-1")).await.unwrap();
        queue.put(make_task("task-2")).await.unwrap();

        let task1 = queue.get().await.unwrap();
        assert_eq!(task1.id(), "task-1");

        let task2 = queue.get().await.unwrap();
        assert_eq!(task2.id(), "task-2");
    }

    #[tokio::test]
    async fn test_queue_full() {
        let queue = TaskQueue::new(2);

        queue.put(make_task("task-1")).await.unwrap();
        queue.put(make_task("task-2")).await.unwrap();

        let result = queue.put(make_task("task-3")).await;
        assert!(matches!(result, Err(QueueError::QueueFull(2))));
    }

    #[tokio::test]
    async fn test_queue_shutdown() {
        let queue = TaskQueue::new(10);
        let queue_clone = queue.clone();

        // Spawn a task that waits for a value
        let handle = tokio::spawn(async move {
            queue_clone.get().await
        });

        // Give it time to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Shutdown the queue
        queue.shutdown().await;

        // The waiting task should return None
        let result = handle.await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_queue_clear() {
        let queue = TaskQueue::new(10);

        queue.put(make_task("task-1")).await.unwrap();
        queue.put(make_task("task-2")).await.unwrap();
        queue.put(make_task("task-3")).await.unwrap();

        let tasks = queue.clear().await;
        assert_eq!(tasks.len(), 3);
        assert!(queue.is_empty().await);
    }
}
