# F09: Scriptable CLI

**Owner:** `cli` agent. **Depends on:** F00; integrates F01–F03 and F07. **Output:** human and script entry points.

## Scope

- Implement service startup and project/repository registration using F01 validation.
- Start the F08 coordinator with `tesselate serve`; the stdio adapter connects to the resulting owner-only Unix socket.
- Provide inspection for projects, runs, tasks, pending human requests, and worktrees; read commands support machine-readable JSON.
- Resolve human requests through F07, export timelines through F10, and inspect/clean up worktrees through F03.
- Apply the same core authorization and transition rules as MCP. Present actionable errors for invalid configuration, Git conflicts, and rejected cleanup.
- Document credential/bootstrap commands defined in F00; avoid printing secrets in logs or exports.

## Acceptance

- CLI commands cover every operation listed in spec §7 and work against the one coordinator state store.
- JSON reads parse consistently and do not mix diagnostics into stdout.
- A human request can be listed and resolved; the governed workflow then continues.
- Ordinary cleanup refuses a dirty worktree, and the CLI shows why.

**Boundary:** CLI does not own policy, persistence, Git mutations, or artifact rendering.

The service and one-shot commands use the same state directory and SQLite database. Harnesses launch `tesselate-mcp` with `TESSELATE_SOCKET=<state-dir>/coordinator.sock` and a locally supplied `TESSELATE_CREDENTIAL`. CLI commands accept `--state-dir` and `--credential-file` (or the corresponding `TESSELATE_STATE_DIR` and `TESSELATE_CREDENTIAL_FILE` environment variables). Credentials are shown only on first issuance and stored under the state directory with owner-only Unix permissions.
