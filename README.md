# ansible-worker

A worker that runs Ansible playbooks on demand. It listens for task requests over MQTT or HTTP, runs `ansible-playbook`, and reports progress and results back to the source.

Built for setups where one or more workers pull jobs from a shared queue. With MQTT, a group of workers can share a subscription so each task is handled by exactly one worker.

## How it works

1. The worker connects to a broker (MQTT) or polls an endpoint (HTTP) for new tasks.
2. Incoming task requests are validated and pushed onto a bounded queue.
3. A single executor pulls tasks off the queue one at a time and runs `ansible-playbook`.
4. While a playbook runs, its stdout is parsed to track task counts (ok, changed, failed, skipped, unreachable).
5. Status updates are published back as the task moves through `queued`, `running`, and a final state (`success`, `failed`, `cancelled`, or `timeout`).

## Requirements

- Rust to build
- `ansible-playbook` on the worker host
- `git` if you use the `git_pull` option
- An MQTT broker, or an HTTP endpoint that serves tasks

## Build

```sh
cargo build --release
```

The binary lands in `target/release/ansible-worker`.

## Configuration

Copy the example and edit it:

```sh
cp config.example.yaml config.yaml
```

Any string value supports `${VAR_NAME}` environment variable expansion, which is resolved before the config is parsed. Use it for secrets so they stay out of the file:

```yaml
mqtt:
  password: "${MQTT_PASSWORD}"
```

### Full MQTT example

```yaml
transport: mqtt

mqtt:
  host: "mqtt.example.com"
  port: 1883                                  # optional, default 1883
  username: "worker"                          # optional
  password: "${MQTT_PASSWORD}"                # optional
  keepalive: 60                               # optional, seconds, default 60
  tls_enabled: false                          # optional, default false
  # tls_ca_cert: "/etc/ansible-worker/ca.crt"        # optional, defaults to system roots
  # tls_client_cert: "/etc/ansible-worker/client.crt" # optional, enables mutual TLS
  # tls_client_key: "/etc/ansible-worker/client.key"  # optional, required with the cert above

worker:
  group_name: "production"
  # worker_id: "worker-01"                    # optional, stable id for this worker
  playbook_directory: "/opt/ansible/playbooks"
  topic_prefix: "ansible"                     # optional, default "ansible"
  # shared_subscription: true                 # optional, default true (MQTT only)
  # suffix_status_with_worker_id: false       # optional, default false (MQTT only)
  max_queue_size: 100                         # optional, default 100
  task_timeout: 3600                          # optional, seconds, default 3600
  # ansible_playbook_path: "ansible-playbook" # optional, default "ansible-playbook"
  # git_path: "git"                           # optional, default "git"

log_level: "INFO"                             # optional, default INFO
```

### Full HTTP example

```yaml
transport: http

http:
  base_url: "https://api.example.com/ansible"
  poll_interval: 5                            # optional, seconds, default 5
  timeout: 30                                 # optional, seconds, default 30
  connect_timeout: 10                         # optional, seconds, default 10
  # Authentication (choose one):
  bearer_token: "${API_TOKEN}"                # optional, bearer auth
  # username: "worker"                        # optional, basic auth
  # password: "${API_PASSWORD}"               # optional, basic auth
  # TLS:
  # tls_ca_cert: "/etc/ansible-worker/ca.crt"         # optional, custom CA
  # tls_client_cert: "/etc/ansible-worker/client.crt" # optional, mutual TLS
  # tls_client_key: "/etc/ansible-worker/client.key"  # optional, required with the cert above
  # tls_insecure: false                       # optional, skip cert verification, not for production

worker:
  group_name: "production"
  playbook_directory: "/opt/ansible/playbooks"
  max_queue_size: 100
  task_timeout: 3600

log_level: "INFO"
```

### Options reference

Top level:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `transport` | no | `mqtt` | `mqtt` or `http` |
| `mqtt` | with mqtt transport | | MQTT connection block |
| `http` | with http transport | | HTTP connection block |
| `worker` | yes | | Worker behaviour |
| `log_level` | no | `INFO` | `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR` |

`worker`:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `group_name` | yes | | Group used for routing and shared subscriptions |
| `worker_id` | no | | Stable identifier for this worker; required when `suffix_status_with_worker_id` is enabled |
| `playbook_directory` | yes | | Must exist; playbooks and file inventories live here |
| `topic_prefix` | no | `ansible` | MQTT topic prefix |
| `shared_subscription` | no | `true` | MQTT only. `true`: tasks are load-balanced across the group (one worker per task). `false`: every worker receives every task |
| `suffix_status_with_worker_id` | no | `false` | MQTT only. Append `worker_id` to the status topic (`.../status/<worker_id>`). Requires `worker_id` |
| `max_queue_size` | no | `100` | Tasks beyond this are rejected |
| `task_timeout` | no | `3600` | Default per-task timeout in seconds |
| `ansible_playbook_path` | no | `ansible-playbook` | Path to the binary |
| `git_path` | no | `git` | Path to the binary, used for `git_pull` |

`mqtt`:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `host` | yes | | Broker hostname or IP |
| `port` | no | `1883` | |
| `username` | no | | |
| `password` | no | | |
| `keepalive` | no | `60` | Seconds |
| `tls_enabled` | no | `false` | |
| `tls_ca_cert` | no | system roots | CA for server verification |
| `tls_client_cert` | no | | Client cert for mutual TLS |
| `tls_client_key` | no | | Client key for mutual TLS |

`http`:

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `base_url` | yes | | API base URL |
| `poll_interval` | no | `5` | Seconds between polls |
| `timeout` | no | `30` | Request timeout in seconds |
| `connect_timeout` | no | `10` | Connection timeout in seconds |
| `bearer_token` | no | | Bearer auth |
| `username` | no | | Basic auth |
| `password` | no | | Basic auth |
| `tls_ca_cert` | no | system roots | Custom CA |
| `tls_client_cert` | no | | Client cert for mutual TLS |
| `tls_client_key` | no | | Client key for mutual TLS |
| `tls_insecure` | no | `false` | Skip certificate verification |

## Running

```sh
ansible-worker --config config.yaml
```

Check a config without starting the worker:

```sh
ansible-worker --config config.yaml --validate
```

The worker shuts down cleanly on `Ctrl+C` or `SIGTERM`. On shutdown it stops accepting work, cancels any queued tasks, and reports them as `cancelled`.

## Transports

### MQTT

By default, workers in a group subscribe to a shared subscription so each task is delivered to exactly one worker. The subscription is made at QoS 2 (exactly once):

```
$share/ansible-worker-<group>/<prefix>/<group>/tasks
```

Set `worker.shared_subscription: false` to disable this. The worker then subscribes to the plain task topic and every worker in the group receives every task (fan-out):

```
<prefix>/<group>/tasks
```

Status is published per task at QoS 1 (at least once) with the retain flag set, so the broker keeps the last status for each task:

```
<prefix>/<group>/tasks/<task_id>/status
```

Set `worker.suffix_status_with_worker_id: true` (and configure `worker.worker_id`) to append the worker identifier, so each worker publishes to its own status subtopic:

```
<prefix>/<group>/tasks/<task_id>/status/<worker_id>
```

`<group>` is `worker.group_name` and `<prefix>` is `worker.topic_prefix`. So with the example config above, workers receive on `$share/ansible-worker-production/ansible/production/tasks` and publish to `ansible/production/tasks/<task_id>/status`.

The worker uses a clean session and connects with client IDs `ansible-worker-<group>-<uuid>` for the receiver and `ansible-worker-pub-<group>-<uuid>` for the publisher. The payloads are documented under [MQTT payloads](#mqtt-payloads).

### HTTP

The worker polls `GET <base_url>/tasks` on an interval. The endpoint returns a single task or an array of tasks as JSON, or `204 No Content` when there is nothing to do. Status updates are sent with `POST <base_url>/tasks/<task_id>/status`. Every request carries an `X-Worker-Group` header.

## Task request

A task request is JSON and is the same for both transports. Over MQTT it is the payload published to the tasks topic; over HTTP it is what the poll endpoint returns. Only `task_id`, `playbook`, and `inventory` are required:

```json
{
  "task_id": "deploy-2026-001",
  "playbook": "deploy.yml",
  "inventory": "production",
  "extra_vars": { "version": "1.4.0" },
  "limit": "webservers",
  "tags": ["deploy"],
  "skip_tags": ["debug"],
  "verbosity": 1,
  "check_mode": false,
  "diff_mode": false,
  "forks": 10,
  "timeout": 1800,
  "git_pull": true
}
```

- `playbook` and file based `inventory` are resolved relative to the configured playbook directory. An inventory ending in `,` or one without a path separator is treated as an inline host list and passed through.
- `git_pull` runs `git pull` in the playbook directory before the run. This is guarded by a lock so concurrent tasks do not collide.
- `timeout` overrides the worker default for that task.

Field reference:

| Field | Type | Required | Default | Maps to |
| --- | --- | --- | --- | --- |
| `task_id` | string | yes | | identifier echoed back in status |
| `playbook` | string | yes | | playbook path |
| `inventory` | string | yes | | `-i` |
| `extra_vars` | object | no | | `-e` (passed as JSON) |
| `limit` | string | no | | `--limit` |
| `tags` | string[] | no | | `--tags` |
| `skip_tags` | string[] | no | | `--skip-tags` |
| `verbosity` | integer | no | `0` | `-v`, `-vv`, ... |
| `check_mode` | boolean | no | `false` | `--check` |
| `diff_mode` | boolean | no | `false` | `--diff` |
| `forks` | integer | no | | `--forks` |
| `timeout` | integer | no | `worker.task_timeout` | per-task timeout in seconds |
| `git_pull` | boolean | no | `false` | run `git pull` first |

## MQTT payloads

There are two payloads on the wire: the task request the worker subscribes to, and the status updates it publishes. Both are JSON.

### Inbound: task request

Published by your scheduler to the tasks topic and consumed by one worker in the group:

```
topic: ansible/production/tasks          (subscribed as $share/ansible-worker-production/ansible/production/tasks)
qos:   2
```

The body is a [task request](#task-request). Malformed JSON or a request missing a required field is logged and dropped, with no status published.

### Outbound: status updates

Published per task as it progresses. Sent at QoS 1 with retain set, so a late subscriber still sees the last known state:

```
topic: ansible/production/tasks/<task_id>/status
qos:   1
retain: true
```

A status payload looks like this (here, mid run):

```json
{
  "task_id": "deploy-2026-001",
  "state": "running",
  "created_at": "2026-06-15T09:30:00.123456Z",
  "started_at": "2026-06-15T09:30:01.000000Z",
  "completed_at": null,
  "duration_seconds": null,
  "tasks_total": 4,
  "tasks_ok": 3,
  "tasks_changed": 1,
  "tasks_failed": 0,
  "tasks_skipped": 0,
  "tasks_unreachable": 0,
  "return_code": null,
  "error_message": null
}
```

Status fields:

| Field | Type | Notes |
| --- | --- | --- |
| `task_id` | string | matches the request |
| `state` | string | `queued`, `running`, `success`, `failed`, `cancelled`, or `timeout` |
| `created_at` | string | RFC 3339 UTC, set when the task is accepted |
| `started_at` | string \| null | set when execution begins |
| `completed_at` | string \| null | set when execution ends |
| `duration_seconds` | number \| null | wall-clock run time, set on completion |
| `tasks_total` | integer | Ansible tasks seen |
| `tasks_ok` | integer | `ok` results |
| `tasks_changed` | integer | `changed` results |
| `tasks_failed` | integer | `failed` results |
| `tasks_skipped` | integer | `skipped` results |
| `tasks_unreachable` | integer | `unreachable` results |
| `return_code` | integer \| null | exit code of `ansible-playbook` |
| `error_message` | string \| null | captured failure detail |

The first update for a task is normally `running`. A `running` update is also sent as each Ansible task begins, so counters climb during the run. Exactly one terminal update follows.

Successful run:

```json
{
  "task_id": "deploy-2026-001",
  "state": "success",
  "created_at": "2026-06-15T09:30:00.123456Z",
  "started_at": "2026-06-15T09:30:01.000000Z",
  "completed_at": "2026-06-15T09:31:12.456000Z",
  "duration_seconds": 71.456,
  "tasks_total": 12,
  "tasks_ok": 30,
  "tasks_changed": 4,
  "tasks_failed": 0,
  "tasks_skipped": 2,
  "tasks_unreachable": 0,
  "return_code": 0,
  "error_message": null
}
```

Failed run, with the captured error:

```json
{
  "task_id": "deploy-2026-001",
  "state": "failed",
  "created_at": "2026-06-15T09:30:00.123456Z",
  "started_at": "2026-06-15T09:30:01.000000Z",
  "completed_at": "2026-06-15T09:30:44.900000Z",
  "duration_seconds": 43.9,
  "tasks_total": 8,
  "tasks_ok": 14,
  "tasks_changed": 1,
  "tasks_failed": 1,
  "tasks_skipped": 0,
  "tasks_unreachable": 0,
  "return_code": 2,
  "error_message": "Task 'Restart nginx': fatal: [web01]: FAILED! => {\"changed\": false, \"msg\": \"Unable to start service nginx\"}\n  Message: Unable to start service nginx"
}
```

Validation failures (a playbook path outside the playbook directory, a missing playbook, template markers in `extra_vars`, or a failed `git pull`) produce the same `failed` shape with `started_at` null, `return_code` null, and the reason in `error_message`.

### Rejection: queue full

If the queue is at `max_queue_size`, the task is rejected immediately and a single `failed` status is published to its status topic:

```json
{
  "task_id": "deploy-2026-001",
  "state": "failed",
  "created_at": "2026-06-15T09:30:00.123456Z",
  "started_at": null,
  "completed_at": null,
  "duration_seconds": null,
  "tasks_total": 0,
  "tasks_ok": 0,
  "tasks_changed": 0,
  "tasks_failed": 0,
  "tasks_skipped": 0,
  "tasks_unreachable": 0,
  "return_code": null,
  "error_message": "Queue full (max: 100)"
}
```

### Cancellation: shutdown

On `Ctrl+C` or `SIGTERM`, any tasks still queued are cancelled and published as `cancelled` with `completed_at` set. A task already running is killed and reported as `cancelled` as well.

## Safety

- Playbook and inventory paths are checked to stay inside the playbook directory, so a request cannot reach files elsewhere on the host.
- `extra_vars` are scanned for Jinja2 markers (`{{`, `}}`, `{%`, and so on) and rejected if found, to avoid template injection through task input.

## License

MIT
