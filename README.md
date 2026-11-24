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

## OMP integration

For OMP sessions working in repositories under `~/Development`, install the
bundled OMP extension and feature skill once:

```sh
tesselate install omp
```

This uses `~/Development/.tesselate` for private coordinator state,
credentials, run handoffs, worktrees, and artifacts. The installer places an
extension, skill, and credential-file-path configuration in the active user's
OMP agent directory; it adds no credentials or OMP files to a project repository.
When running a binary from this checkout instead of an installed `tesselate` on `PATH`, use
`./target/debug/tesselate install omp`. `install omp`, `uninstall omp`,
`project ensure`, and `project provision` default to this shared state directory;
other CLI commands retain the existing `.tesselate` default. Pass `--state-dir`
explicitly when calling those commands by hand against the shared installation.

Installation follows the active `OMP_PROFILE` / `PI_PROFILE`, if set; otherwise
it targets the default `~/.omp/agent` profile. Install separately for each named
profile. The generated OMP configuration stores credential **file paths**, not
bearer values. The installed CLI recovers an expired, non-revoked supervisor
session only from its owner-only local credential file; socket/MCP authentication
continues to reject expired credentials.

Start OMP from a repository, or from `~/Development`, and ask for a new feature
in a named repository. The integration asks for the feature goal and acceptance
criteria, then uses Tesselate for the run, assigned coding worktrees, independent
reviews, and durable history. `/feature` is the explicit alternative when a
plain-language request is ambiguous. To register a repository directly:

```sh
tesselate project ensure --repo ~/Development/bitwars
```

To opt into copying ignored local files for a repository, configure an
integration-only policy once. Bitwars' existing `.workmux.yaml` lists these paths:

```sh
tesselate project provision --repo ~/Development/bitwars \
  --copy .env --copy .env.local \
  --copy apps/web/.env --copy apps/web/.env.local \
  --copy apps/web/.env.development --copy apps/web/.env.development.local \
  --copy services/rts-engine/.env \
  --copy packages/content/.revisions
```

The private policy lives beside the generated project configuration under
`~/Development/.tesselate/projects/`; repeat the same command safely, or use
`--replace` to change its copy list. Other repositories can list their own
Git-ignored relative paths. Tesselate shows present and missing optional paths
for approval before creating a run, then snapshots files into the integration
worktree with owner-only permissions. It neither copies to coder/reviewer
worktrees nor executes workmux `post_create`, `pre_merge`, or `pre_remove` hooks,
which can run arbitrary commands. Symlinks, tracked paths, and changed
destinations stop provisioning for inspection; file contents are never logged.

The agent proposes an integration-only validation command from repository
instructions. At the validation prompt, press Enter to use that proposal or
type an override; a separate confirmation displays the exact command before
the run starts. If no command was proposed, the agent must investigate and
retry rather than starting without validation.

The extension launches separate OMP processes for the coder and two reviewers;
ordinary OMP subagents share their parent's MCP connection and cannot represent
independent Tesselate roles. A task worktree is not the integration branch.
Confirm an integration-only validation command explicitly; do not run shared
services in task worktrees. Bitwars' configured ignored files are copied into
the integration checkout only after approval; absent optional files are skipped.
No automatic push, PR creation, or merge into local `main`.

Workspace inspection reports both `capture_ready` and `clean`. Committed work
can be captured when branch/HEAD identity matches and there are no tracked
changes or non-ignored untracked files; ignored build output and local handoffs
do not block review. Cleanup remains stricter: `clean` is false while any
ignored files remain, so deleting a worktree cannot discard local data.

```sh
tesselate uninstall omp
```

Uninstall removes only Tesselate-owned, unmodified OMP integration files; it
preserves coordinator state, credentials, project configurations, and worktrees.
Restart OMP to load an installed or removed extension.

## Documentation

- [Product and technical specification](docs/spec.md)
- [M001 milestone plan and feature workflow](docs/milestones/m001-mvp/README.md)
- [End-to-end scenario and observed results](docs/milestones/m001-mvp/f11-e2e-results.md)
- [Architecture decision records](docs/adrs/)
