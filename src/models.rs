//! Data types exchanged over the wire: incoming task requests, outgoing task
//! status, and the combined [`Task`] used internally.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Errors produced when parsing or validating task data.
#[derive(Error, Debug)]
pub enum ModelError {
    /// A required field was absent or the payload failed to parse.
    #[error("Missing required field: {0}")]
    MissingField(String),
    /// A task state value could not be interpreted.
    #[error("Invalid task state: {0}")]
    InvalidState(String),
}

/// Task execution state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Accepted and waiting in the queue.
    Queued,
    /// Currently running `ansible-playbook`.
    Running,
    /// Finished with a zero exit code and no failed or unreachable hosts.
    Success,
    /// Finished with a non-zero exit code or with failures.
    Failed,
    /// Cancelled, typically because the worker is shutting down.
    Cancelled,
    /// Killed after exceeding its timeout.
    Timeout,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Queued => write!(f, "queued"),
            TaskState::Running => write!(f, "running"),
            TaskState::Success => write!(f, "success"),
            TaskState::Failed => write!(f, "failed"),
            TaskState::Cancelled => write!(f, "cancelled"),
            TaskState::Timeout => write!(f, "timeout"),
        }
    }
}

/// An incoming task request, deserialized from a transport payload.
///
/// Only `task_id`, `playbook`, and `inventory` are required; all other fields
/// map onto `ansible-playbook` command-line options.
#[derive(Debug, Clone, Deserialize)]
pub struct TaskRequest {
    /// Caller-supplied unique identifier for the task.
    pub task_id: String,
    /// Playbook path, resolved relative to the configured playbook directory.
    pub playbook: String,
    /// Inventory: a file path under the playbook directory, or an inline host
    /// list (ending in `,` or containing no path separator).
    pub inventory: String,
    /// Extra variables passed via `-e` as JSON.
    #[serde(default)]
    pub extra_vars: Option<HashMap<String, serde_json::Value>>,
    /// Value for `--limit`.
    pub limit: Option<String>,
    /// Tags passed via `--tags`.
    pub tags: Option<Vec<String>>,
    /// Tags passed via `--skip-tags`.
    pub skip_tags: Option<Vec<String>>,
    /// Verbosity level, rendered as `-v`, `-vv`, and so on.
    #[serde(default)]
    pub verbosity: u8,
    /// Run with `--check` (dry run) when true.
    #[serde(default)]
    pub check_mode: bool,
    /// Run with `--diff` when true.
    #[serde(default)]
    pub diff_mode: bool,
    /// Value for `--forks`.
    pub forks: Option<u32>,
    /// Per-task timeout in seconds, overriding the worker default.
    pub timeout: Option<u64>,
    /// Run `git pull` in the playbook directory before executing.
    #[serde(default)]
    pub git_pull: bool,
}

impl TaskRequest {
    /// Parse a task request from JSON bytes
    pub fn from_json(data: &[u8]) -> Result<Self, ModelError> {
        let request: TaskRequest =
            serde_json::from_slice(data).map_err(|e| ModelError::MissingField(e.to_string()))?;

        // Validate required fields
        if request.task_id.is_empty() {
            return Err(ModelError::MissingField("task_id".to_string()));
        }
        if request.playbook.is_empty() {
            return Err(ModelError::MissingField("playbook".to_string()));
        }
        if request.inventory.is_empty() {
            return Err(ModelError::MissingField("inventory".to_string()));
        }

        Ok(request)
    }
}

/// A serializable snapshot of a task's progress and outcome, published back to
/// the task source.
#[derive(Debug, Clone, Serialize)]
pub struct TaskStatus {
    /// Identifier of the task this status refers to.
    pub task_id: String,
    /// Current execution state.
    pub state: TaskState,
    /// When the task was created (accepted into the queue).
    pub created_at: DateTime<Utc>,
    /// When execution started, if it has.
    pub started_at: Option<DateTime<Utc>>,
    /// When execution finished, if it has.
    pub completed_at: Option<DateTime<Utc>>,
    /// Wall-clock duration in seconds, set once the task completes.
    pub duration_seconds: Option<f64>,
    /// Number of Ansible tasks seen.
    pub tasks_total: u32,
    /// Count of `ok` results.
    pub tasks_ok: u32,
    /// Count of `changed` results.
    pub tasks_changed: u32,
    /// Count of `failed` results.
    pub tasks_failed: u32,
    /// Count of `skipped` results.
    pub tasks_skipped: u32,
    /// Count of `unreachable` results.
    pub tasks_unreachable: u32,
    /// Exit code of `ansible-playbook`, if it ran to completion.
    pub return_code: Option<i32>,
    /// Captured error detail when the task failed.
    pub error_message: Option<String>,
}

impl TaskStatus {
    /// Create a fresh `Queued` status for the given task ID.
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            state: TaskState::Queued,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            duration_seconds: None,
            tasks_total: 0,
            tasks_ok: 0,
            tasks_changed: 0,
            tasks_failed: 0,
            tasks_skipped: 0,
            tasks_unreachable: 0,
            return_code: None,
            error_message: None,
        }
    }

    /// Calculate and set duration when task completes
    pub fn calculate_duration(&mut self) {
        if let (Some(started), Some(completed)) = (self.started_at, self.completed_at) {
            let duration = completed - started;
            self.duration_seconds = Some(duration.num_milliseconds() as f64 / 1000.0);
        }
    }

    /// Convert to JSON bytes for publishing
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}

/// Internal task representation combining request and status
#[derive(Debug, Clone)]
pub struct Task {
    /// The original request that created this task.
    pub request: TaskRequest,
    /// The task's mutable, publishable status.
    pub status: TaskStatus,
}

impl Task {
    /// Wrap a request together with a fresh `Queued` status.
    pub fn new(request: TaskRequest) -> Self {
        let status = TaskStatus::new(request.task_id.clone());
        Self { request, status }
    }

    /// Get task ID
    pub fn id(&self) -> &str {
        &self.request.task_id
    }

    /// Get current state
    pub fn state(&self) -> TaskState {
        self.status.state
    }

    /// Set state to running
    pub fn start(&mut self) {
        self.status.state = TaskState::Running;
        self.status.started_at = Some(Utc::now());
    }

    /// Set state to success
    pub fn succeed(&mut self, return_code: i32) {
        self.status.state = TaskState::Success;
        self.status.completed_at = Some(Utc::now());
        self.status.return_code = Some(return_code);
        self.status.calculate_duration();
    }

    /// Set state to failed
    pub fn fail(&mut self, return_code: Option<i32>, error_message: Option<String>) {
        self.status.state = TaskState::Failed;
        self.status.completed_at = Some(Utc::now());
        self.status.return_code = return_code;
        self.status.error_message = error_message;
        self.status.calculate_duration();
    }

    /// Set state to timeout
    pub fn timeout(&mut self) {
        self.status.state = TaskState::Timeout;
        self.status.completed_at = Some(Utc::now());
        self.status.calculate_duration();
    }

    /// Set state to cancelled
    pub fn cancel(&mut self) {
        self.status.state = TaskState::Cancelled;
        self.status.completed_at = Some(Utc::now());
        self.status.calculate_duration();
    }

    /// Increment task counters based on ansible event
    pub fn increment_total(&mut self) {
        self.status.tasks_total += 1;
    }

    /// Record an `ok` result.
    pub fn increment_ok(&mut self) {
        self.status.tasks_ok += 1;
    }

    /// Record a `changed` result.
    pub fn increment_changed(&mut self) {
        self.status.tasks_changed += 1;
    }

    /// Record a `failed` result.
    pub fn increment_failed(&mut self) {
        self.status.tasks_failed += 1;
    }

    /// Record a `skipped` result.
    pub fn increment_skipped(&mut self) {
        self.status.tasks_skipped += 1;
    }

    /// Record an `unreachable` result.
    pub fn increment_unreachable(&mut self) {
        self.status.tasks_unreachable += 1;
    }

    /// Update stats from final playbook stats
    pub fn update_stats(
        &mut self,
        ok: u32,
        changed: u32,
        failed: u32,
        skipped: u32,
        unreachable: u32,
    ) {
        self.status.tasks_ok = ok;
        self.status.tasks_changed = changed;
        self.status.tasks_failed = failed;
        self.status.tasks_skipped = skipped;
        self.status.tasks_unreachable = unreachable;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_request_parsing() {
        let json = r#"{
            "task_id": "test-123",
            "playbook": "deploy.yml",
            "inventory": "production",
            "extra_vars": {"version": "1.0"},
            "limit": "webservers",
            "verbosity": 2
        }"#;

        let request = TaskRequest::from_json(json.as_bytes()).unwrap();
        assert_eq!(request.task_id, "test-123");
        assert_eq!(request.playbook, "deploy.yml");
        assert_eq!(request.inventory, "production");
        assert_eq!(request.limit, Some("webservers".to_string()));
        assert_eq!(request.verbosity, 2);
    }

    #[test]
    fn test_task_state_transitions() {
        let request = TaskRequest::from_json(
            r#"{"task_id": "test", "playbook": "test.yml", "inventory": "test"}"#.as_bytes(),
        )
        .unwrap();

        let mut task = Task::new(request);
        assert_eq!(task.state(), TaskState::Queued);

        task.start();
        assert_eq!(task.state(), TaskState::Running);
        assert!(task.status.started_at.is_some());

        task.succeed(0);
        assert_eq!(task.state(), TaskState::Success);
        assert!(task.status.completed_at.is_some());
        assert!(task.status.duration_seconds.is_some());
    }

    fn make_task() -> Task {
        let request = TaskRequest::from_json(
            r#"{"task_id": "t", "playbook": "p.yml", "inventory": "h,"}"#.as_bytes(),
        )
        .unwrap();
        Task::new(request)
    }

    #[test]
    fn test_fail_records_code_and_message() {
        let mut task = make_task();
        task.start();
        task.fail(Some(2), Some("boom".to_string()));

        assert_eq!(task.state(), TaskState::Failed);
        assert_eq!(task.status.return_code, Some(2));
        assert_eq!(task.status.error_message.as_deref(), Some("boom"));
        assert!(task.status.completed_at.is_some());
        assert!(task.status.duration_seconds.is_some());
    }

    #[test]
    fn test_timeout_and_cancel_set_completion() {
        let mut timed_out = make_task();
        timed_out.start();
        timed_out.timeout();
        assert_eq!(timed_out.state(), TaskState::Timeout);
        assert!(timed_out.status.completed_at.is_some());

        let mut cancelled = make_task();
        cancelled.cancel();
        assert_eq!(cancelled.state(), TaskState::Cancelled);
        assert!(cancelled.status.completed_at.is_some());
    }

    #[test]
    fn test_counters_and_update_stats() {
        let mut task = make_task();
        task.increment_total();
        task.increment_ok();
        task.increment_changed();
        task.increment_failed();
        task.increment_skipped();
        task.increment_unreachable();

        assert_eq!(task.status.tasks_total, 1);
        assert_eq!(task.status.tasks_ok, 1);
        assert_eq!(task.status.tasks_changed, 1);
        assert_eq!(task.status.tasks_failed, 1);
        assert_eq!(task.status.tasks_skipped, 1);
        assert_eq!(task.status.tasks_unreachable, 1);

        // update_stats overwrites the running counts with final recap values.
        task.update_stats(30, 4, 0, 2, 0);
        assert_eq!(task.status.tasks_ok, 30);
        assert_eq!(task.status.tasks_changed, 4);
        assert_eq!(task.status.tasks_failed, 0);
        assert_eq!(task.status.tasks_skipped, 2);
        assert_eq!(task.status.tasks_unreachable, 0);
    }

    #[test]
    fn test_status_to_json_serializes_state_lowercase() {
        let mut task = make_task();
        task.start();
        task.succeed(0);

        let bytes = task.status.to_json().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["task_id"], "t");
        assert_eq!(value["state"], "success");
        assert_eq!(value["return_code"], 0);
        assert!(value["duration_seconds"].is_number());
        assert!(value["error_message"].is_null());
    }
}
