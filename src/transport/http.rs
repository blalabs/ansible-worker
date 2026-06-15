//! HTTP transport: polls an endpoint for tasks and posts status updates back
//! to it, with optional bearer/basic auth and TLS.

use super::{StatusPublisher, TaskReceiver};
use crate::config::Config;
use crate::models::{Task, TaskRequest, TaskStatus};
use crate::task_queue::{QueueError, TaskQueue};
use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::{Client, ClientBuilder};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

/// HTTP-based transport for task communication
pub struct HttpTransport {
    config: Arc<Config>,
    client: Client,
}

impl HttpTransport {
    /// Build a transport and its HTTP client from configuration. Returns an
    /// error if the HTTP section is missing or the TLS material fails to load.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let http_config = config.http.as_ref().ok_or_else(|| {
            anyhow::anyhow!("HTTP configuration required for HTTP transport")
        })?;

        let mut builder = ClientBuilder::new()
            .timeout(Duration::from_secs(http_config.timeout))
            .connect_timeout(Duration::from_secs(http_config.connect_timeout));

        // Configure TLS
        if http_config.tls_insecure {
            warn!("TLS certificate verification is disabled - not recommended for production");
            builder = builder.danger_accept_invalid_certs(true);
        }

        // Load custom CA certificate if provided
        if let Some(ca_path) = &http_config.tls_ca_cert {
            let ca_cert = std::fs::read(ca_path)
                .with_context(|| format!("Failed to read CA certificate: {}", ca_path))?;
            let cert = reqwest::Certificate::from_pem(&ca_cert)
                .with_context(|| format!("Failed to parse CA certificate: {}", ca_path))?;
            builder = builder.add_root_certificate(cert);
        }

        // Configure client certificate for mutual TLS
        if let (Some(cert_path), Some(key_path)) =
            (&http_config.tls_client_cert, &http_config.tls_client_key)
        {
            let cert_pem = std::fs::read(cert_path)
                .with_context(|| format!("Failed to read client certificate: {}", cert_path))?;
            let key_pem = std::fs::read(key_path)
                .with_context(|| format!("Failed to read client key: {}", key_path))?;

            // Combine cert and key for reqwest Identity
            let mut identity_pem = cert_pem;
            identity_pem.extend_from_slice(&key_pem);

            let identity = reqwest::Identity::from_pem(&identity_pem)
                .context("Failed to create identity from client certificate and key")?;
            builder = builder.identity(identity);
        }

        let client = builder.build().context("Failed to build HTTP client")?;

        Ok(Self { config, client })
    }

    /// Get the tasks URL
    fn tasks_url(&self) -> String {
        let http = self.config.http.as_ref().unwrap();
        format!("{}/tasks", http.base_url.trim_end_matches('/'))
    }

    /// Get the status URL for a specific task
    fn status_url(&self, task_id: &str) -> String {
        let http = self.config.http.as_ref().unwrap();
        format!(
            "{}/tasks/{}/status",
            http.base_url.trim_end_matches('/'),
            task_id
        )
    }

    /// Build request with authentication headers
    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let http = self.config.http.as_ref().unwrap();

        let request = if let Some(token) = &http.bearer_token {
            request.bearer_auth(token)
        } else {
            request
        };

        if let (Some(username), Some(password)) = (&http.username, &http.password) {
            request.basic_auth(username, Some(password))
        } else {
            request
        }
    }
}

#[async_trait]
impl TaskReceiver for HttpTransport {
    async fn run(
        &self,
        task_queue: TaskQueue,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let http = self.config.http.as_ref().ok_or_else(|| {
            anyhow::anyhow!("HTTP configuration required")
        })?;

        let poll_interval = Duration::from_secs(http.poll_interval);
        let tasks_url = self.tasks_url();

        info!("Starting HTTP task poller, URL: {}", tasks_url);

        loop {
            // Check for shutdown
            if *shutdown_rx.borrow() {
                info!("HTTP poller received shutdown signal");
                break;
            }

            // Poll for tasks
            match self.poll_tasks(&tasks_url, &task_queue).await {
                Ok(count) => {
                    if count > 0 {
                        info!("Received {} task(s) from HTTP endpoint", count);
                    }
                }
                Err(e) => {
                    error!("Failed to poll for tasks: {}", e);
                }
            }

            // Wait for next poll or shutdown
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("HTTP poller received shutdown signal while waiting");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl HttpTransport {
    async fn poll_tasks(&self, url: &str, task_queue: &TaskQueue) -> Result<usize> {
        let request = self.client.get(url);
        let request = self.authenticate(request);

        let response = request
            .header("X-Worker-Group", &self.config.worker.group_name)
            .send()
            .await
            .context("Failed to send HTTP request")?;

        if response.status() == reqwest::StatusCode::NO_CONTENT {
            // No tasks available
            return Ok(0);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("HTTP request failed with status {}: {}", status, body);
        }

        // Try to parse as array of tasks or single task
        let body = response.bytes().await.context("Failed to read response body")?;

        // First try parsing as array
        let tasks: Vec<TaskRequest> = match serde_json::from_slice(&body) {
            Ok(tasks) => tasks,
            Err(_) => {
                // Try parsing as single task
                match serde_json::from_slice(&body) {
                    Ok(task) => vec![task],
                    Err(e) => {
                        // Check if it's an empty response
                        if body.is_empty() {
                            return Ok(0);
                        }
                        anyhow::bail!("Failed to parse task response: {}", e);
                    }
                }
            }
        };

        let mut count = 0;
        for request in tasks {
            let task_id = request.task_id.clone();
            let task = Task::new(request);

            match task_queue.put(task).await {
                Ok(()) => {
                    debug!("Task {} added to queue", task_id);
                    count += 1;
                }
                Err(QueueError::QueueFull(max)) => {
                    warn!("Queue full (max: {}), rejecting task {}", max, task_id);
                    // Send failure status
                    let mut status = TaskStatus::new(task_id);
                    status.state = crate::models::TaskState::Failed;
                    status.error_message = Some(format!("Queue full (max: {})", max));
                    let _ = self.publish_status_internal(&status).await;
                }
                Err(QueueError::Shutdown) => {
                    warn!("Queue shutdown, stopping task reception");
                    break;
                }
            }
        }

        Ok(count)
    }

    async fn publish_status_internal(&self, status: &TaskStatus) -> Result<()> {
        let url = self.status_url(&status.task_id);
        let request = self.client.post(&url);
        let request = self.authenticate(request);

        let response = request
            .header("X-Worker-Group", &self.config.worker.group_name)
            .json(status)
            .send()
            .await
            .context("Failed to send status update")?;

        if !response.status().is_success() {
            let status_code = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to publish status, HTTP {}: {}",
                status_code,
                body
            );
        }

        Ok(())
    }
}

/// HTTP status publisher
pub struct HttpStatusPublisher {
    config: Arc<Config>,
    client: Client,
}

impl HttpStatusPublisher {
    /// Build a status publisher and its HTTP client from configuration. Returns
    /// an error if the HTTP section is missing or the TLS material fails to load.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let http_config = config.http.as_ref().ok_or_else(|| {
            anyhow::anyhow!("HTTP configuration required for HTTP transport")
        })?;

        let mut builder = ClientBuilder::new()
            .timeout(Duration::from_secs(http_config.timeout))
            .connect_timeout(Duration::from_secs(http_config.connect_timeout));

        // Configure TLS (same as HttpTransport)
        if http_config.tls_insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(ca_path) = &http_config.tls_ca_cert {
            let ca_cert = std::fs::read(ca_path)
                .with_context(|| format!("Failed to read CA certificate: {}", ca_path))?;
            let cert = reqwest::Certificate::from_pem(&ca_cert)
                .with_context(|| format!("Failed to parse CA certificate: {}", ca_path))?;
            builder = builder.add_root_certificate(cert);
        }

        if let (Some(cert_path), Some(key_path)) =
            (&http_config.tls_client_cert, &http_config.tls_client_key)
        {
            let cert_pem = std::fs::read(cert_path)
                .with_context(|| format!("Failed to read client certificate: {}", cert_path))?;
            let key_pem = std::fs::read(key_path)
                .with_context(|| format!("Failed to read client key: {}", key_path))?;

            let mut identity_pem = cert_pem;
            identity_pem.extend_from_slice(&key_pem);

            let identity = reqwest::Identity::from_pem(&identity_pem)
                .context("Failed to create identity from client certificate and key")?;
            builder = builder.identity(identity);
        }

        let client = builder.build().context("Failed to build HTTP client")?;

        Ok(Self { config, client })
    }

    fn status_url(&self, task_id: &str) -> String {
        let http = self.config.http.as_ref().unwrap();
        format!(
            "{}/tasks/{}/status",
            http.base_url.trim_end_matches('/'),
            task_id
        )
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let http = self.config.http.as_ref().unwrap();

        let request = if let Some(token) = &http.bearer_token {
            request.bearer_auth(token)
        } else {
            request
        };

        if let (Some(username), Some(password)) = (&http.username, &http.password) {
            request.basic_auth(username, Some(password))
        } else {
            request
        }
    }
}

#[async_trait]
impl StatusPublisher for HttpStatusPublisher {
    async fn publish(&self, status: &TaskStatus) -> Result<()> {
        let url = self.status_url(&status.task_id);
        let request = self.client.post(&url);
        let request = self.authenticate(request);

        let response = request
            .header("X-Worker-Group", &self.config.worker.group_name)
            .json(status)
            .send()
            .await
            .context("Failed to send status update")?;

        if !response.status().is_success() {
            let status_code = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to publish status, HTTP {}: {}",
                status_code,
                body
            );
        }

        debug!("Published status for task {} to {}", status.task_id, url);
        Ok(())
    }
}
