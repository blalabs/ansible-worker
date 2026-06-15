//! Pluggable transports for receiving tasks and publishing status.
//!
//! A transport implements [`TaskReceiver`] to feed the queue and
//! [`StatusPublisher`] to report results. Two implementations are provided:
//! [`MqttTransport`]/[`MqttStatusPublisher`] and
//! [`HttpTransport`]/[`HttpStatusPublisher`].

pub mod http;
pub mod mqtt;

use crate::models::TaskStatus;
use crate::task_queue::TaskQueue;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

/// Trait for receiving tasks from a remote source
#[async_trait]
pub trait TaskReceiver: Send + Sync {
    /// Run the task receiver loop, putting received tasks into the queue
    /// This should run until shutdown is signaled
    async fn run(
        &self,
        task_queue: TaskQueue,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()>;
}

/// Trait for publishing task status updates
#[async_trait]
pub trait StatusPublisher: Send + Sync {
    /// Publish a status update for a task
    async fn publish(&self, status: &TaskStatus) -> Result<()>;

    /// Connect to the remote service (if needed)
    async fn connect(&self) -> Result<()> {
        Ok(())
    }
}

/// Re-export transport types
pub use http::{HttpStatusPublisher, HttpTransport};
pub use mqtt::{MqttStatusPublisher, MqttTransport};
