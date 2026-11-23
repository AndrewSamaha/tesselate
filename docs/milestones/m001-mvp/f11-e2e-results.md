# F11 scripted end-to-end demonstration

The acceptance scenario lives in `tests/f11_e2e.rs`. It starts the actual `tesselate serve` coordinator and four independent `tesselate-mcp` stdio processes, and sends MCP JSON-RPC `initialize`, `tools/call`, and reconnect requests. All state and Git repositories are created in a unique temporary directory for each test run.

## Reproduce

```sh
cargo test --test f11_e2e -- --nocapture
```

The test requires a Unix host (the coordinator uses an owner-only Unix-domain socket) and an installed `git` executable. It creates two temporary repositories with one committed base each, registers both through `tesselate project register`, issues credentials through the CLI, and starts one shared coordinator backed by its SQLite state directory. It deletes only its own temporary fixture directory when the test exits.

## Scenarios and assertions

| Spec §11 criterion | Scenario | Assertion performed |
| --- | --- | --- |
| 1 | CLI registers a project with two local repositories; the supervisor creates and assigns two tasks. | Both repository records are listed and the project/run/task operations succeed through their public adapters. |
| 2 | Two coder credentials claim separate tasks and create worktrees in different repositories before either task is submitted. | The returned workspaces point to distinct worktree paths and both commits are captured and submitted. |
| 3 | Two separate reviewer processes inspect both first-round changes; reviewer A records a concrete finding on repo A and an explicit pass on repo B, reviewer B records explicit passes. | Review records are persisted under the requested rounds, with two independent reviewer identities per task. |
| 4 | Supervisor routes the finding through a durable task message; coder A reads it, changes the file, commits, and submits revision round two. | The revision references the prior finding ID; both reviewers explicitly pass the revised round. |
| 5 | The autonomous run reaches completion with the two latest reviewed commit refs and an explicit project integration step. A second run exceeds its retry budget. | The first run is `completed`; the second is `stopped` with a persisted retry-limit reason, and its `remaining_task_ids` explicitly contains the unfinished task. |
| 6 | Coordinator is killed and restarted after workspace creation, task submissions, and empty pending review rounds. Four MCP processes reconnect. | Tasks/owners, round commit refs, workspace inspections, messages, and cursors remain readable; consuming from the saved cursor yields no duplicates. |
| 7 | CLI exports the completed run through F10. The test builds relationships from exported event and record JSONL alone, without querying the service or database for the reconstruction assertions. | Both schemas and counts validate; records link run→tasks, tasks→repositories/commit refs, assignments→actors, messages→tasks, rounds→prior findings, reviews→rounds, findings→dispositions, and the completed run→latest reviewed refs. Expected event types and Markdown links are present, repeat export has identical bytes, and no bearer or repository file content appears. |
| 8 | A second project configures `[human].completion_approval=true`; the run completion attempt creates a gate. | CLI lists and resolves the request, after which the same run completes. |
| 9 | CLI attempts cleanup of a worktree with an untracked file. | Cleanup fails with an uncommitted-changes diagnostic and the worktree remains present. After removing the fixture's own dirty file, cleanup succeeds. |

## Observed run

On 2026-11-23, `cargo test --test f11_e2e -- --nocapture` passed the full scenario on the integrated F00–F10 code. The scenario created two repositories, four credentialed agent sessions, an autonomous run with two tasks and three review rounds, a limit-stopped run, and a separate CLI-resolved gated run. No scenario was skipped. The test uses deterministic fixture operations and asserts resulting state rather than recording machine-specific UUIDs or commit hashes.

The demonstration records commit references and a human-readable integration instruction; it never merges either fixture project's primary branch. Credential values stay in temporary owner-only CLI credential files and process environment for the lifetime of the local test; the artifact assertions verify the supervisor credential is absent from exported event JSONL.

## Current boundary

The E2E harness owns fixture orchestration and proof of cross-feature behavior. F01–F10 own policy, persistence, Git operations, lifecycle rules, transport, CLI, and artifact rendering. `tesselate-mcp` and the coordinator socket are Unix-only at this stage. The CLI's JSON output and MCP's structured tool payloads are the public interface exercised here.
