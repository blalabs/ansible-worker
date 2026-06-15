//! # ansible-worker
//!
//! A worker that runs Ansible playbooks on demand. It receives task requests
//! over a pluggable transport (MQTT or HTTP), runs `ansible-playbook` for each
//! request, and publishes status updates back to the source.
//!
//! ## Architecture
//!
//! - [`config`] loads and validates the YAML configuration.
//! - [`models`] defines the task request, status, and state types exchanged
//!   over the wire.
//! - [`task_queue`] holds incoming work in a bounded, async FIFO queue.
//! - [`transport`] defines the [`TaskReceiver`](transport::TaskReceiver) and
//!   [`StatusPublisher`](transport::StatusPublisher) traits along with their
//!   MQTT and HTTP implementations.
//! - [`executor`] pulls tasks off the queue and runs `ansible-playbook`,
//!   parsing its output to track progress.

#![warn(missing_docs)]

pub mod config;
pub mod executor;
pub mod models;
pub mod task_queue;
pub mod transport;

/// The crate version, read from `CARGO_PKG_VERSION` at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
