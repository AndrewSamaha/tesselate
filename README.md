# Tesselate

Tesselate is a local coordination service for coding agents. It gives a supervisor, coding agents, and independent reviewers a shared workflow across local Git repositories. Agent harnesses run the agents; Tesselate coordinates their assignments, worktrees, messages, reviews, and decisions.

The MVP is local and Unix-only. It uses a single SQLite database and does not host models, connect to remote repositories, or merge changes into a project's primary branch.

## What it does

- Coordinates runs, dependent tasks, assignments, leases, and bounded retries.
- Creates isolated Git worktrees and records submitted commit references.
- Enforces independent review rounds, findings, and finding dispositions.
- Supports optional human approval gates resolved through the CLI.
- Serves concurrent agent sessions through MCP over a local Unix socket.
- Persists workflow history in SQLite and exports portable JSON Lines and Markdown artifacts.

## Requirements

- Rust toolchain and Cargo
- Git
- Unix-like operating system (the coordinator and MCP adapter use Unix-domain sockets)

## Build and test

```sh
cargo build
cargo test
```

The end-to-end acceptance scenario creates temporary local Git repositories and launches the real coordinator and MCP adapter processes:

```sh
cargo test --test f11_e2e -- --nocapture
```

## Quick start

Bootstrap the first supervisor credential. Tesselate writes it to a private credential file and returns the credential once in the JSON response. Keep that response private.

```sh
cargo run --bin tesselate -- credential bootstrap --actor-name supervisor
```

Create a project configuration file. Repository paths and `artifact_dir` may be relative to the configuration file's directory.

```toml
name = "example"
artifact_dir = ".tesselate-artifacts"

[[repositories]]
id = "app"
path = "/path/to/app"

[workflow]
max_duration_hours = 24
max_concurrent_assignments = 4
max_task_retries = 2
max_review_rounds = 3

[review]
min_independent_reviewers = 2

# Optional. Omit [human] for autonomous operation.
[human]
completion_approval = false
high_severity_deferral_approval = false
```

Register the project with the `credential_file` path returned by bootstrap:

```sh
cargo run --bin tesselate -- \
  --credential-file /path/from/bootstrap/supervisor.token \
  project register --config ./project.toml --idempotency-key register-example
```

Start the shared coordinator in one terminal:

```sh
cargo run --bin tesselate -- serve
```

The default state directory is `.tesselate`; set `--state-dir` or `TESSELATE_STATE_DIR` to use another location. CLI commands and the coordinator use the same SQLite database in that directory.

Issue credentials for agent sessions using the supervisor credential. Each issued credential is stored under the state directory and shown only on first issuance:

```sh
cargo run --bin tesselate -- \
  --credential-file /path/from/bootstrap/supervisor.token \
  credential issue --actor-name coder-1 --role coder --idempotency-key issue-coder-1
```

Configure an MCP harness to launch `tesselate-mcp` with the issued credential and the coordinator socket. For example, the process needs these environment variables:

```text
TESSELATE_SOCKET=/path/to/.tesselate/coordinator.sock
TESSELATE_CREDENTIAL=<credential for this agent>
```

The adapter speaks MCP on stdio and forwards requests to the shared coordinator. Never put credentials in tool arguments or logs. Use `tesselate call` for any versioned coordination tool, or the purpose-built `inspect`, `human`, `workspace`, and `artifact` commands. Mutating calls require an idempotency key.

## Documentation

- [Product and technical specification](docs/spec.md)
- [M001 milestone plan and feature workflow](docs/milestones/m001-mvp/README.md)
- [End-to-end scenario and observed results](docs/milestones/m001-mvp/f11-e2e-results.md)
- [Architecture decision records](docs/adrs/)
