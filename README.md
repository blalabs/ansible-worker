# ansible-worker

Execute Ansible playbooks triggered via MQTT messages with real-time status reporting.

## Installation

```bash
pip install .
```

Or for development:

```bash
pip install -e ".[dev]"
```

## Configuration

Copy `config.example.yaml` to `config.yaml` and adjust the settings:

```yaml
mqtt:
  host: "mqtt.example.com"
  port: 1883
  username: "ansible-worker"
  password: "${MQTT_PASSWORD}"  # Environment variable expansion supported
  keepalive: 60
  tls_enabled: false

worker:
  group_name: "production"
  playbook_directory: "/opt/ansible/playbooks"
  topic_prefix: "ansible"
  max_queue_size: 100
  task_timeout: 3600

log_level: "INFO"
```

## Usage

```bash
# Run with default config.yaml
ansible-worker

# Run with custom config file
ansible-worker -c /path/to/config.yaml

# Validate configuration
ansible-worker --validate

# Run as a module
python -m ansible_worker -c config.yaml
```

## MQTT Topics

The topic prefix is configurable via `worker.topic_prefix` (default: `ansible`).

### Subscribe

The worker subscribes to receive task requests using MQTT 5.0 shared subscriptions:

- **Topic:** `$share/ansible-worker-<group>/<prefix>/<group>/tasks` (QoS 2)
- **Example:** `$share/ansible-worker-production/ansible/production/tasks`

The `$share/` prefix enables distributed task handling - the broker delivers each message to only ONE worker in the group, providing automatic load balancing.

### Publish

Status updates are published to:

- **Topic:** `<prefix>/<group>/tasks/<task_id>/status` (QoS 1, retained)
- **Example:** `ansible/production/tasks/a1b2c3d4e5f67890/status`

## Distributed Task Handling

When running multiple workers in the same group, the MQTT broker automatically distributes tasks among them using **shared subscriptions** (MQTT 5.0 feature).

### How It Works

1. All workers in a group subscribe to `$share/ansible-worker-<group>/<topic>`
2. When a task message is published, the broker delivers it to exactly ONE worker
3. The first available (idle) worker receives and processes the task
4. Other workers don't see the message at all - no duplicate work

### Benefits

- **No application-level locking** - the broker handles distribution
- **Automatic load balancing** - idle workers get tasks first
- **No duplicate execution** - each task is delivered to exactly one worker
- **Simple scaling** - just start more workers to increase capacity
- **No external dependencies** - uses built-in MQTT 5.0 feature

### Requirements

Your MQTT broker must support MQTT 5.0 shared subscriptions:

## Message Schemas

### Task Request (Input)

Publish to `<prefix>/<group>/tasks`:

```json
{
  "task_id": "a1b2c3d4e5f67890",
  "playbook": "deploy/application.yml",
  "inventory": "production",
  "extra_vars": {"app_version": "2.5.0"},
  "limit": "webservers",
  "tags": ["deploy"],
  "skip_tags": [],
  "verbosity": 0,
  "check_mode": false,
  "diff_mode": false,
  "forks": 5,
  "timeout": 1800
}
```

**Required fields:** `task_id`, `playbook`, `inventory`

The `task_id` must be provided by the controller application. This ID is used for status reporting and correlating task execution.

### Task Status (Output)

Published to `<prefix>/<group>/tasks/<task_id>/status`:

```json
{
  "task_id": "a1b2c3d4e5f67890",
  "state": "running",
  "created_at": "2026-01-21T10:30:00.000Z",
  "started_at": "2026-01-21T10:30:05.000Z",
  "completed_at": null,
  "duration_seconds": 45.5,
  "tasks_total": 25,
  "tasks_ok": 20,
  "tasks_changed": 3,
  "tasks_failed": 0,
  "tasks_skipped": 2,
  "tasks_unreachable": 0,
  "return_code": null,
  "error_message": null
}
```

**States:** `queued`, `running`, `success`, `failed`, `cancelled`, `timeout`

## Error Handling

| Error | Action |
|-------|--------|
| MQTT disconnect | Auto-reconnect with exponential backoff |
| Invalid JSON/missing fields | Log error, discard message |
| Playbook not found | Mark task `failed`, publish status |
| Execution error | Mark task `failed` with error message |
| Queue full | Reject with `failed` status |
| Timeout | Mark task `timeout` |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type checking
mypy ansible_worker

# Linting
ruff check ansible_worker
```

## License

MIT
