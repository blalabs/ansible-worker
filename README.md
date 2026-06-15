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

Values support environment variable expansion with `${VAR_NAME}`, which is handy for secrets:

```yaml
mqtt:
  password: "${MQTT_PASSWORD}"
```

See `config.example.yaml` for the full set of options. The main sections are:

- `transport`: `mqtt` or `http`
- `mqtt` / `http`: connection details for the chosen transport, including TLS and authentication
- `worker`: group name, playbook directory, queue size, task timeout, and binary paths
- `log_level`: `TRACE`, `DEBUG`, `INFO`, `WARN`, or `ERROR`

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

Workers subscribe to a shared topic so tasks are load balanced across a group:

```
$share/ansible-worker-<group>/<prefix>/<group>/tasks
```

Status is published per task to:

```
<prefix>/<group>/tasks/<task_id>/status
```

### HTTP

The worker polls `GET <base_url>/tasks` on an interval. The endpoint returns a single task or an array of tasks as JSON, or `204 No Content` when there is nothing to do. Status updates are sent with `POST <base_url>/tasks/<task_id>/status`. Every request carries an `X-Worker-Group` header.

## Task format

A task request is JSON. Only `task_id`, `playbook`, and `inventory` are required:

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

## Safety

- Playbook and inventory paths are checked to stay inside the playbook directory, so a request cannot reach files elsewhere on the host.
- `extra_vars` are scanned for Jinja2 markers (`{{`, `}}`, `{%`, and so on) and rejected if found, to avoid template injection through task input.

## License

MIT
