//! Runs queued tasks by invoking `ansible-playbook`.
//!
//! The [`Executor`] pulls one task at a time off the [`TaskQueue`], validates
//! its paths and variables, optionally runs `git pull`, then spawns
//! `ansible-playbook` and parses its stdout to track per-task counts and
//! capture error output. Status updates are sent over a channel to be
//! published by the transport.

use crate::config::Config;
use crate::models::{Task, TaskStatus};
use crate::task_queue::TaskQueue;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, watch, Mutex};
use tracing::{debug, error, info, warn};

/// Status update sent from executor
#[derive(Debug, Clone)]
pub struct ExecutorStatusUpdate {
    /// The latest status snapshot for a task.
    pub status: TaskStatus,
}

/// Execution result containing exit code and captured error messages
struct ExecutionResult {
    exit_code: i32,
    error_messages: Vec<String>,
}

/// Executor runs ansible playbooks from the task queue
pub struct Executor {
    config: Arc<Config>,
    task_queue: TaskQueue,
    status_tx: mpsc::Sender<ExecutorStatusUpdate>,
    shutdown_rx: watch::Receiver<bool>,
    git_lock: Arc<Mutex<()>>,
}

impl Executor {
    /// Create an executor that consumes from `task_queue`, reports status on
    /// `status_tx`, and stops when `shutdown_rx` is set to `true`.
    pub fn new(
        config: Arc<Config>,
        task_queue: TaskQueue,
        status_tx: mpsc::Sender<ExecutorStatusUpdate>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            task_queue,
            status_tx,
            shutdown_rx,
            git_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Validate that a path is within the allowed base directory (prevents path traversal)
    fn validate_path_within_base(&self, requested_path: &str, base_dir: &Path) -> Result<PathBuf, String> {
        // Canonicalize the base directory
        let canonical_base = match base_dir.canonicalize() {
            Ok(p) => p,
            Err(e) => return Err(format!("Failed to resolve base directory: {}", e)),
        };

        // Join and canonicalize the requested path
        let full_path = base_dir.join(requested_path);
        let canonical_path = match full_path.canonicalize() {
            Ok(p) => p,
            Err(_) => {
                // File doesn't exist yet, check parent directory
                // This handles cases where we're checking if a path *would* be valid
                if let Some(parent) = full_path.parent() {
                    if let Ok(canonical_parent) = parent.canonicalize() {
                        if !canonical_parent.starts_with(&canonical_base) {
                            return Err("Path traversal detected: path escapes base directory".to_string());
                        }
                    }
                }
                return Err(format!("Path does not exist: {}", full_path.display()));
            }
        };

        // Verify the canonical path is within the base directory
        if !canonical_path.starts_with(&canonical_base) {
            return Err("Path traversal detected: path escapes base directory".to_string());
        }

        Ok(canonical_path)
    }

    /// Sanitize extra vars to prevent Jinja2 template injection
    fn sanitize_extra_vars(
        &self,
        extra_vars: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, String> {
        let mut sanitized = std::collections::HashMap::new();

        for (key, value) in extra_vars {
            // Check key for template patterns
            if Self::contains_template_pattern(key) {
                return Err(format!("Template injection detected in key: {}", key));
            }

            // Recursively check and sanitize values
            let sanitized_value = Self::sanitize_value(value)?;
            sanitized.insert(key.clone(), sanitized_value);
        }

        Ok(sanitized)
    }

    /// Check if a string contains Jinja2 template patterns
    fn contains_template_pattern(s: &str) -> bool {
        s.contains("{{") || s.contains("}}") || s.contains("{%") || s.contains("%}")
            || s.contains("{#") || s.contains("#}")
    }

    /// Recursively sanitize a JSON value
    fn sanitize_value(value: &serde_json::Value) -> Result<serde_json::Value, String> {
        match value {
            serde_json::Value::String(s) => {
                if Self::contains_template_pattern(s) {
                    return Err(format!("Template injection detected in value: {}", s));
                }
                Ok(value.clone())
            }
            serde_json::Value::Array(arr) => {
                let mut sanitized = Vec::new();
                for item in arr {
                    sanitized.push(Self::sanitize_value(item)?);
                }
                Ok(serde_json::Value::Array(sanitized))
            }
            serde_json::Value::Object(obj) => {
                let mut sanitized = serde_json::Map::new();
                for (k, v) in obj {
                    if Self::contains_template_pattern(k) {
                        return Err(format!("Template injection detected in key: {}", k));
                    }
                    sanitized.insert(k.clone(), Self::sanitize_value(v)?);
                }
                Ok(serde_json::Value::Object(sanitized))
            }
            // Numbers, booleans, and null are safe
            _ => Ok(value.clone()),
        }
    }

    /// Run the executor loop
    pub async fn run(&mut self) {
        info!("Executor started");

        loop {
            // Check for shutdown
            if *self.shutdown_rx.borrow() {
                info!("Executor received shutdown signal");
                break;
            }

            // Get next task with timeout
            let task = tokio::select! {
                task = self.task_queue.get_timeout(Duration::from_secs(1)) => task,
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("Executor received shutdown signal while waiting");
                        break;
                    }
                    continue;
                }
            };

            if let Some(task) = task {
                info!("Executing task: {}", task.id());
                if let Err(e) = self.execute_task(task).await {
                    error!("Task execution error: {}", e);
                }
            }
        }

        info!("Executor stopped");
    }

    /// Execute a single task
    async fn execute_task(&mut self, mut task: Task) -> Result<()> {
        let task_id = task.id().to_string();
        let base_dir = Path::new(&self.config.worker.playbook_directory);

        // Validate playbook path (prevents path traversal)
        let playbook_path = match self.validate_path_within_base(&task.request.playbook, base_dir) {
            Ok(path) => path,
            Err(e) => {
                task.fail(None, Some(format!("Invalid playbook path: {}", e)));
                self.send_status(&task.status).await;
                return Ok(());
            }
        };

        // Validate inventory path (must be within playbook directory or a simple hostname pattern)
        let inventory = &task.request.inventory;
        let is_simple_inventory = inventory.ends_with(',') ||
            (!inventory.contains('/') && !inventory.contains('\\'));

        if !is_simple_inventory {
            // It's a file path, validate it's within the base directory
            if let Err(e) = self.validate_path_within_base(inventory, base_dir) {
                task.fail(None, Some(format!("Invalid inventory path: {}", e)));
                self.send_status(&task.status).await;
                return Ok(());
            }
        }

        // Sanitize extra vars to prevent template injection
        if let Some(extra_vars) = &task.request.extra_vars {
            if let Err(e) = self.sanitize_extra_vars(extra_vars) {
                task.fail(None, Some(format!("Invalid extra vars: {}", e)));
                self.send_status(&task.status).await;
                return Ok(());
            }
        }

        // Optionally run git pull (with lock to prevent race conditions)
        if task.request.git_pull {
            info!("Running git pull for task {} (acquiring lock)", task_id);
            let _git_guard = self.git_lock.lock().await;
            info!("Git lock acquired for task {}", task_id);
            if let Err(e) = self.git_pull().await {
                task.fail(None, Some(format!("Git pull failed: {}", e)));
                self.send_status(&task.status).await;
                return Ok(());
            }
        }

        // Verify playbook file exists (after potential git pull)
        if !playbook_path.exists() {
            task.fail(
                None,
                Some("Playbook not found".to_string()),
            );
            self.send_status(&task.status).await;
            return Ok(());
        }

        // Mark as running
        task.start();
        self.send_status(&task.status).await;

        // Build command arguments (use validated playbook path)
        let args = self.build_args(&task, &playbook_path);

        info!(
            "Running ansible-playbook with args: {:?}",
            args.join(" ")
        );

        // Determine timeout
        let timeout = task
            .request
            .timeout
            .unwrap_or(self.config.worker.task_timeout);

        // Run ansible-playbook
        let result = self
            .run_ansible_playbook(&args, &mut task, Duration::from_secs(timeout))
            .await;

        match result {
            Ok(exec_result) => {
                if exec_result.exit_code == 0
                    && task.status.tasks_failed == 0
                    && task.status.tasks_unreachable == 0
                {
                    task.succeed(exec_result.exit_code);
                    info!("Task {} completed successfully", task_id);
                } else {
                    // Combine error messages if any were captured
                    let error_message = if exec_result.error_messages.is_empty() {
                        None
                    } else {
                        Some(exec_result.error_messages.join("\n"))
                    };
                    task.fail(Some(exec_result.exit_code), error_message);
                    info!("Task {} failed with exit code {}", task_id, exec_result.exit_code);
                }
            }
            Err(ExecutionError::Timeout) => {
                task.timeout();
                warn!("Task {} timed out", task_id);
            }
            Err(ExecutionError::Cancelled) => {
                task.cancel();
                info!("Task {} was cancelled", task_id);
            }
            Err(ExecutionError::Other(e)) => {
                task.fail(None, Some(e.to_string()));
                error!("Task {} failed: {}", task_id, e);
            }
        }

        self.send_status(&task.status).await;
        Ok(())
    }

    /// Build command-line arguments for ansible-playbook
    fn build_args(&self, task: &Task, playbook_path: &Path) -> Vec<String> {
        let mut args = Vec::new();

        // Playbook path (already validated)
        args.push(playbook_path.to_string_lossy().to_string());

        // Inventory
        args.push("-i".to_string());
        args.push(task.request.inventory.clone());

        // Extra vars
        if let Some(extra_vars) = &task.request.extra_vars {
            if let Ok(json) = serde_json::to_string(extra_vars) {
                args.push("-e".to_string());
                args.push(json);
            }
        }

        // Limit
        if let Some(limit) = &task.request.limit {
            args.push("--limit".to_string());
            args.push(limit.clone());
        }

        // Tags
        if let Some(tags) = &task.request.tags {
            if !tags.is_empty() {
                args.push("--tags".to_string());
                args.push(tags.join(","));
            }
        }

        // Skip tags
        if let Some(skip_tags) = &task.request.skip_tags {
            if !skip_tags.is_empty() {
                args.push("--skip-tags".to_string());
                args.push(skip_tags.join(","));
            }
        }

        // Verbosity
        if task.request.verbosity > 0 {
            let v = "-".to_string() + &"v".repeat(task.request.verbosity as usize);
            args.push(v);
        }

        // Check mode
        if task.request.check_mode {
            args.push("--check".to_string());
        }

        // Diff mode
        if task.request.diff_mode {
            args.push("--diff".to_string());
        }

        // Forks
        if let Some(forks) = task.request.forks {
            args.push("--forks".to_string());
            args.push(forks.to_string());
        }

        args
    }

    /// Run ansible-playbook and process output
    async fn run_ansible_playbook(
        &mut self,
        args: &[String],
        task: &mut Task,
        timeout: Duration,
    ) -> Result<ExecutionResult, ExecutionError> {
        let mut cmd = Command::new(&self.config.worker.ansible_playbook_path);
        cmd.args(args)
            .current_dir(&self.config.worker.playbook_directory)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("ANSIBLE_FORCE_COLOR", "false")
            .env("ANSIBLE_NOCOLOR", "true");

        let mut child = cmd.spawn().map_err(|e| ExecutionError::Other(e.into()))?;

        let stdout = child.stdout.take().expect("stdout not captured");
        let stderr = child.stderr.take().expect("stderr not captured");

        let stdout_reader = BufReader::new(stdout);
        let stderr_reader = BufReader::new(stderr);

        let mut stdout_lines = stdout_reader.lines();
        let mut stderr_lines = stderr_reader.lines();

        // Track task stats and errors from output
        let mut current_task_name = String::new();
        let mut error_messages: Vec<String> = Vec::new();
        let mut capturing_error = false;
        let mut current_error_lines: Vec<String> = Vec::new();

        let timeout_future = tokio::time::sleep(timeout);
        tokio::pin!(timeout_future);

        loop {
            tokio::select! {
                // Check for shutdown
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        let _ = child.kill().await;
                        return Err(ExecutionError::Cancelled);
                    }
                }

                // Check for timeout
                _ = &mut timeout_future => {
                    let _ = child.kill().await;
                    return Err(ExecutionError::Timeout);
                }

                // Read stdout
                line = stdout_lines.next_line() => {
                    match line {
                        Ok(Some(line)) => {
                            self.process_output_line(
                                &line,
                                task,
                                &mut current_task_name,
                                &mut error_messages,
                                &mut capturing_error,
                                &mut current_error_lines,
                            ).await;
                        }
                        Ok(None) => {
                            // Flush any remaining error being captured
                            if capturing_error && !current_error_lines.is_empty() {
                                error_messages.push(current_error_lines.join("\n"));
                            }
                            // stdout closed, wait for process to exit
                            break;
                        }
                        Err(e) => {
                            warn!("Error reading stdout: {}", e);
                        }
                    }
                }

                // Read stderr
                line = stderr_lines.next_line() => {
                    match line {
                        Ok(Some(line)) => {
                            debug!("stderr: {}", line);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Error reading stderr: {}", e);
                        }
                    }
                }
            }
        }

        // Wait for process to complete
        let status = child
            .wait()
            .await
            .map_err(|e| ExecutionError::Other(e.into()))?;

        Ok(ExecutionResult {
            exit_code: status.code().unwrap_or(-1),
            error_messages,
        })
    }

    /// Process a line of ansible-playbook output
    async fn process_output_line(
        &self,
        line: &str,
        task: &mut Task,
        current_task: &mut String,
        error_messages: &mut Vec<String>,
        capturing_error: &mut bool,
        current_error_lines: &mut Vec<String>,
    ) {
        let line = line.trim();
        if line.is_empty() {
            return;
        }

        debug!("ansible output: {}", line);

        // Parse ansible output to track task progress
        // Ansible output format:
        // TASK [task name] ****
        // ok: [hostname]
        // changed: [hostname]
        // failed: [hostname]
        // skipping: [hostname]
        // fatal: [hostname]: UNREACHABLE!
        // fatal: [hostname]: FAILED! => {"msg": "error message", ...}

        if line.starts_with("TASK [") {
            // New task starting - flush any previous error
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }
            *capturing_error = false;

            // Extract task name
            if let Some(end) = line.find(']') {
                *current_task = line[6..end].to_string();
                task.increment_total();
                debug!("Task started: {}", current_task);

                // Send periodic status update
                self.send_status(&task.status).await;
            }
        } else if line.starts_with("PLAY [") || line.starts_with("PLAY RECAP") {
            // New play or recap - flush any previous error
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }
            *capturing_error = false;

            if line.starts_with("PLAY RECAP") {
                debug!("Play recap reached");
            }
        } else if line.starts_with("ok:") {
            // Flush any previous error
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }
            *capturing_error = false;
            task.increment_ok();
        } else if line.starts_with("changed:") {
            // Flush any previous error
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }
            *capturing_error = false;
            task.increment_ok();
            task.increment_changed();
        } else if line.starts_with("failed:") || line.starts_with("fatal:") {
            // Flush any previous error first
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }

            // Start capturing this error
            *capturing_error = true;

            // Build error context with task name
            let error_context = if !current_task.is_empty() {
                format!("Task '{}': {}", current_task, line)
            } else {
                line.to_string()
            };
            current_error_lines.push(error_context);

            // Try to extract the error message from JSON if present
            // Format: fatal: [host]: FAILED! => {"msg": "error message", ...}
            if let Some(json_start) = line.find("=> {") {
                let json_str = &line[json_start + 3..];
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                    if let Some(msg) = json.get("msg").and_then(|m| m.as_str()) {
                        current_error_lines.push(format!("  Message: {}", msg));
                    }
                }
            }

            if line.contains("UNREACHABLE") {
                task.increment_unreachable();
            } else {
                task.increment_failed();
            }
        } else if line.starts_with("skipping:") {
            // Flush any previous error
            if *capturing_error && !current_error_lines.is_empty() {
                error_messages.push(current_error_lines.join("\n"));
                current_error_lines.clear();
            }
            *capturing_error = false;
            task.increment_skipped();
        } else if *capturing_error {
            // Continue capturing multi-line error output
            // Stop if we hit a line that looks like a new status or is just decorative
            if !line.starts_with("...") && !line.chars().all(|c| c == '*' || c == ' ') {
                current_error_lines.push(format!("  {}", line));
            }
        }

        // Parse play recap for final stats
        // Format: hostname : ok=N changed=N unreachable=N failed=N skipped=N
        if line.contains(" : ok=") && line.contains(" changed=") {
            self.parse_recap_line(line, task);
        }
    }

    /// Parse the PLAY RECAP line for final stats
    fn parse_recap_line(&self, line: &str, task: &mut Task) {
        // Format: hostname : ok=N changed=N unreachable=N failed=N skipped=N
        let mut ok: u32 = 0;
        let mut changed: u32 = 0;
        let mut unreachable: u32 = 0;
        let mut failed: u32 = 0;
        let mut skipped: u32 = 0;

        for part in line.split_whitespace() {
            if let Some(val) = part.strip_prefix("ok=") {
                ok += val.parse::<u32>().unwrap_or(0);
            } else if let Some(val) = part.strip_prefix("changed=") {
                changed += val.parse::<u32>().unwrap_or(0);
            } else if let Some(val) = part.strip_prefix("unreachable=") {
                unreachable += val.parse::<u32>().unwrap_or(0);
            } else if let Some(val) = part.strip_prefix("failed=") {
                failed += val.parse::<u32>().unwrap_or(0);
            } else if let Some(val) = part.strip_prefix("skipped=") {
                skipped += val.parse::<u32>().unwrap_or(0);
            }
        }

        // Update task stats (accumulate across hosts)
        task.status.tasks_ok = task.status.tasks_ok.saturating_add(ok);
        task.status.tasks_changed = task.status.tasks_changed.saturating_add(changed);
        task.status.tasks_unreachable = task.status.tasks_unreachable.saturating_add(unreachable);
        task.status.tasks_failed = task.status.tasks_failed.saturating_add(failed);
        task.status.tasks_skipped = task.status.tasks_skipped.saturating_add(skipped);

        debug!(
            "Recap stats: ok={}, changed={}, unreachable={}, failed={}, skipped={}",
            ok, changed, unreachable, failed, skipped
        );
    }

    /// Run git pull in the playbook directory
    async fn git_pull(&self) -> Result<()> {
        let output = Command::new(&self.config.worker.git_path)
            .args(["pull"])
            .current_dir(&self.config.worker.playbook_directory)
            .output()
            .await
            .context("Failed to execute git pull")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("git pull failed: {}", stderr);
        }

        info!("Git pull completed successfully");
        Ok(())
    }

    /// Send a status update
    async fn send_status(&self, status: &TaskStatus) {
        let update = ExecutorStatusUpdate {
            status: status.clone(),
        };
        if let Err(e) = self.status_tx.send(update).await {
            error!("Failed to send status update: {}", e);
        }
    }
}

#[derive(Debug)]
enum ExecutionError {
    Timeout,
    Cancelled,
    Other(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TaskRequest;

    fn make_task() -> Task {
        let request = TaskRequest::from_json(
            r#"{
                "task_id": "test-123",
                "playbook": "test.yml",
                "inventory": "localhost,",
                "verbosity": 2,
                "check_mode": true,
                "limit": "webservers",
                "tags": ["deploy", "configure"],
                "forks": 10
            }"#
            .as_bytes(),
        )
        .unwrap();
        Task::new(request)
    }

    #[test]
    fn test_build_args() {
        let config = Arc::new(Config {
            transport: crate::config::TransportType::Mqtt,
            mqtt: Some(crate::config::MqttConfig {
                host: "localhost".to_string(),
                port: 1883,
                username: None,
                password: None,
                keepalive: 60,
                tls_enabled: false,
                tls_ca_cert: None,
                tls_client_cert: None,
                tls_client_key: None,
                tls_insecure: false,
            }),
            http: None,
            worker: crate::config::WorkerConfig {
                group_name: "test".to_string(),
                playbook_directory: "/tmp".to_string(),
                topic_prefix: "ansible".to_string(),
                max_queue_size: 100,
                task_timeout: 3600,
                ansible_playbook_path: "ansible-playbook".to_string(),
                git_path: "git".to_string(),
            },
            log_level: "INFO".to_string(),
        });

        let (tx, _rx) = mpsc::channel(10);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let queue = TaskQueue::new(10);
        let executor = Executor::new(config, queue, tx, shutdown_rx);

        let task = make_task();
        let playbook_path = Path::new("/tmp/test.yml");
        let args = executor.build_args(&task, playbook_path);

        assert!(args.contains(&"/tmp/test.yml".to_string()));
        assert!(args.contains(&"-i".to_string()));
        assert!(args.contains(&"localhost,".to_string()));
        assert!(args.contains(&"--limit".to_string()));
        assert!(args.contains(&"webservers".to_string()));
        assert!(args.contains(&"--tags".to_string()));
        assert!(args.contains(&"deploy,configure".to_string()));
        assert!(args.contains(&"-vv".to_string()));
        assert!(args.contains(&"--check".to_string()));
        assert!(args.contains(&"--forks".to_string()));
        assert!(args.contains(&"10".to_string()));
    }
}
