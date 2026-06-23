//! Configuration loading and validation.
//!
//! [`Config`] is read from a YAML file with [`Config::load`]. String values
//! support `${VAR_NAME}` environment variable expansion, which is resolved
//! before deserialization.

use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur while loading or validating configuration.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// A referenced `${VAR_NAME}` environment variable was not set.
    #[error("Environment variable '{0}' not set")]
    EnvVarNotSet(String),
    /// The configured playbook directory does not exist on disk.
    #[error("Playbook directory does not exist: {0}")]
    PlaybookDirNotFound(String),
    /// The configured log level is not one of the accepted values.
    #[error("Invalid log level: {0}")]
    InvalidLogLevel(String),
    /// The configured transport type is not recognized.
    #[error("Invalid transport type: {0}")]
    InvalidTransport(String),
    /// The selected transport has no matching configuration section.
    #[error("Missing transport configuration: {0} transport selected but no {0} config provided")]
    MissingTransportConfig(String),
}

/// The transport the worker uses to receive tasks and publish status.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransportType {
    /// Receive tasks from, and publish status to, an MQTT broker.
    #[default]
    Mqtt,
    /// Poll an HTTP endpoint for tasks and post status back to it.
    Http,
}

/// Connection settings for the MQTT transport.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttConfig {
    /// Broker hostname or IP address.
    pub host: String,
    /// Broker port (defaults to 1883).
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    /// Username for broker authentication, if required.
    pub username: Option<String>,
    /// Password for broker authentication, if required.
    pub password: Option<String>,
    /// Keep-alive interval in seconds (defaults to 60).
    #[serde(default = "default_keepalive")]
    pub keepalive: u64,
    /// Whether to connect over TLS.
    #[serde(default)]
    pub tls_enabled: bool,
    /// Path to CA certificate file for TLS verification
    pub tls_ca_cert: Option<String>,
    /// Path to client certificate file for mutual TLS
    pub tls_client_cert: Option<String>,
    /// Path to client private key file for mutual TLS
    pub tls_client_key: Option<String>,
    /// Skip TLS certificate verification (not recommended for production)
    #[serde(default)]
    pub tls_insecure: bool,
}

fn default_mqtt_port() -> u16 {
    1883
}

fn default_keepalive() -> u64 {
    60
}

/// Worker behaviour and execution settings.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    /// Logical group this worker belongs to, used for shared subscriptions and
    /// topic/URL routing.
    pub group_name: String,
    /// Directory that playbooks and file-based inventories must live under.
    pub playbook_directory: String,
    /// Prefix for MQTT topics (defaults to `ansible`).
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    /// Maximum number of queued tasks before new ones are rejected.
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
    /// Default per-task timeout in seconds (defaults to 3600).
    #[serde(default = "default_task_timeout")]
    pub task_timeout: u64,
    /// Path to the `ansible-playbook` binary (defaults to `ansible-playbook`).
    #[serde(default = "default_ansible_playbook_path")]
    pub ansible_playbook_path: String,
    /// Path to the `git` binary used for `git_pull` (defaults to `git`).
    #[serde(default = "default_git_path")]
    pub git_path: String,
}

fn default_topic_prefix() -> String {
    "ansible".to_string()
}

fn default_max_queue_size() -> usize {
    100
}

fn default_task_timeout() -> u64 {
    3600
}

fn default_ansible_playbook_path() -> String {
    "ansible-playbook".to_string()
}

fn default_git_path() -> String {
    "git".to_string()
}

/// Connection settings for the HTTP transport.
#[derive(Debug, Clone, Deserialize)]
pub struct HttpConfig {
    /// Base URL for the HTTP API (e.g., `https://api.example.com/ansible`)
    pub base_url: String,
    /// Poll interval in seconds for fetching new tasks
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Request timeout in seconds
    #[serde(default = "default_http_timeout")]
    pub timeout: u64,
    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    /// Basic auth username
    pub username: Option<String>,
    /// Basic auth password
    pub password: Option<String>,
    /// Bearer token for authentication
    pub bearer_token: Option<String>,
    /// Path to CA certificate file for TLS verification
    pub tls_ca_cert: Option<String>,
    /// Path to client certificate file for mutual TLS
    pub tls_client_cert: Option<String>,
    /// Path to client private key file for mutual TLS
    pub tls_client_key: Option<String>,
    /// Skip TLS certificate verification (not recommended for production)
    #[serde(default)]
    pub tls_insecure: bool,
}

fn default_poll_interval() -> u64 {
    5
}

fn default_http_timeout() -> u64 {
    30
}

fn default_connect_timeout() -> u64 {
    10
}

/// Top-level worker configuration, deserialized from the YAML config file.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Transport type: "mqtt" or "http"
    #[serde(default)]
    pub transport: TransportType,
    /// MQTT configuration (required if transport is mqtt)
    pub mqtt: Option<MqttConfig>,
    /// HTTP configuration (required if transport is http)
    pub http: Option<HttpConfig>,
    /// Worker behaviour and execution settings.
    pub worker: WorkerConfig,
    /// Logging verbosity: TRACE, DEBUG, INFO, WARN, or ERROR.
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String {
    "INFO".to_string()
}

impl Config {
    /// Read the config file at `path`, expand `${VAR_NAME}` references, then
    /// deserialize and validate it.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        // Parse YAML first to get raw values
        let raw: serde_yaml::Value =
            serde_yaml::from_str(&content).context("Failed to parse YAML")?;

        // Expand environment variables
        let expanded = expand_env_vars(raw)?;

        // Deserialize into Config
        let config: Config =
            serde_yaml::from_value(expanded).context("Failed to deserialize config")?;

        // Validate
        config.validate()?;

        Ok(config)
    }

    /// Check that the playbook directory exists, the log level is valid, and
    /// the selected transport has a matching configuration section.
    pub fn validate(&self) -> Result<()> {
        // Validate playbook directory exists
        if !Path::new(&self.worker.playbook_directory).exists() {
            return Err(
                ConfigError::PlaybookDirNotFound(self.worker.playbook_directory.clone()).into(),
            );
        }

        // Validate log level
        let valid_levels = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
        if !valid_levels.contains(&self.log_level.to_uppercase().as_str()) {
            return Err(ConfigError::InvalidLogLevel(self.log_level.clone()).into());
        }

        // Validate transport configuration
        match self.transport {
            TransportType::Mqtt => {
                if self.mqtt.is_none() {
                    return Err(ConfigError::MissingTransportConfig("mqtt".to_string()).into());
                }
            }
            TransportType::Http => {
                if self.http.is_none() {
                    return Err(ConfigError::MissingTransportConfig("http".to_string()).into());
                }
            }
        }

        Ok(())
    }

    /// Get the task subscription topic (shared subscription for load balancing)
    /// Only applicable for MQTT transport
    pub fn task_topic(&self) -> String {
        format!(
            "$share/ansible-worker-{}/{}/{}/tasks",
            self.worker.group_name, self.worker.topic_prefix, self.worker.group_name
        )
    }

    /// Get the status topic for a specific task
    /// Only applicable for MQTT transport
    pub fn status_topic(&self, task_id: &str) -> String {
        format!(
            "{}/{}/tasks/{}/status",
            self.worker.topic_prefix, self.worker.group_name, task_id
        )
    }

    /// Check if using MQTT transport
    pub fn is_mqtt(&self) -> bool {
        self.transport == TransportType::Mqtt
    }

    /// Check if using HTTP transport
    pub fn is_http(&self) -> bool {
        self.transport == TransportType::Http
    }
}

/// Recursively expand environment variables in YAML values
/// Pattern: ${VAR_NAME}
fn expand_env_vars(value: serde_yaml::Value) -> Result<serde_yaml::Value> {
    let re = Regex::new(r"\$\{([^}]+)\}").unwrap();

    match value {
        serde_yaml::Value::String(s) => {
            let mut result = s.clone();
            for cap in re.captures_iter(&s) {
                let var_name = &cap[1];
                let var_value = env::var(var_name)
                    .map_err(|_| ConfigError::EnvVarNotSet(var_name.to_string()))?;
                result = result.replace(&cap[0], &var_value);
            }
            Ok(serde_yaml::Value::String(result))
        }
        serde_yaml::Value::Mapping(map) => {
            let mut new_map = serde_yaml::Mapping::new();
            for (k, v) in map {
                new_map.insert(k, expand_env_vars(v)?);
            }
            Ok(serde_yaml::Value::Mapping(new_map))
        }
        serde_yaml::Value::Sequence(seq) => {
            let new_seq: Result<Vec<_>> = seq.into_iter().map(expand_env_vars).collect();
            Ok(serde_yaml::Value::Sequence(new_seq?))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_expansion() {
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { env::set_var("TEST_VAR", "test_value") };
        let input = serde_yaml::Value::String("prefix_${TEST_VAR}_suffix".to_string());
        let result = expand_env_vars(input).unwrap();
        assert_eq!(
            result,
            serde_yaml::Value::String("prefix_test_value_suffix".to_string())
        );
    }

    #[test]
    fn test_env_var_not_set() {
        let input = serde_yaml::Value::String("${NONEXISTENT_VAR}".to_string());
        let result = expand_env_vars(input);
        assert!(result.is_err());
    }

    fn worker(group: &str, prefix: &str) -> WorkerConfig {
        WorkerConfig {
            group_name: group.to_string(),
            playbook_directory: "/tmp".to_string(),
            topic_prefix: prefix.to_string(),
            max_queue_size: 100,
            task_timeout: 3600,
            ansible_playbook_path: "ansible-playbook".to_string(),
            git_path: "git".to_string(),
        }
    }

    #[test]
    fn test_mqtt_topic_formats() {
        let config = Config {
            transport: TransportType::Mqtt,
            mqtt: None,
            http: None,
            worker: worker("production", "ansible"),
            log_level: "INFO".to_string(),
        };

        assert!(config.is_mqtt());
        assert!(!config.is_http());
        assert_eq!(
            config.task_topic(),
            "$share/ansible-worker-production/ansible/production/tasks"
        );
        assert_eq!(
            config.status_topic("deploy-2026-001"),
            "ansible/production/tasks/deploy-2026-001/status"
        );
    }

    #[test]
    fn test_topics_respect_custom_prefix() {
        let config = Config {
            transport: TransportType::Mqtt,
            mqtt: None,
            http: None,
            worker: worker("staging", "infra"),
            log_level: "INFO".to_string(),
        };

        assert_eq!(
            config.task_topic(),
            "$share/ansible-worker-staging/infra/staging/tasks"
        );
        assert_eq!(config.status_topic("t1"), "infra/staging/tasks/t1/status");
    }

    #[test]
    fn test_http_transport_flags() {
        let config = Config {
            transport: TransportType::Http,
            mqtt: None,
            http: None,
            worker: worker("production", "ansible"),
            log_level: "INFO".to_string(),
        };

        assert!(config.is_http());
        assert!(!config.is_mqtt());
    }
}
