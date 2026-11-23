# M001: Local multi-agent MVP

Source of truth: [product and technical spec](../../spec.md). This milestone turns its acceptance criteria into assignable work. A feature is complete only when its acceptance checks pass; completing all feature documents is not itself proof of the end-to-end workflow.

## Delivery map

| ID | Feature | Primary ownership | Starts after | Size |
| --- | --- | --- | --- | --- |
| F00 | [Crate and contracts](f00-foundation.md) | crate root, shared types | — | M |
| F01 | [Configuration](f01-config.md) | `config` | F00 | S |
| F02 | [Persistence](f02-persistence.md) | `persistence` | F00 | L |
| F03 | [Git workspaces](f03-git.md) | `git` | F00 | M |
| F04 | [Task engine](f04-tasks.md) | `coordination::tasks` | F00, F02 | L |
| F05 | [Sessions and messages](f05-sessions.md) | `coordination::sessions` | F00, F02 | M |
| F06 | [Review workflow](f06-reviews.md) | `coordination::reviews` | F04 | M |
| F07 | [Human gates](f07-human-gates.md) | `coordination::human` | F04 | M |
| F08 | [MCP service](f08-mcp.md) | `mcp` | F00; integrate as F03–F07 land | L |
| F09 | [CLI](f09-cli.md) | `cli` | F00; integrate as F01–F03, F07 land | M |
| F10 | [Artifact export](f10-artifacts.md) | `artifacts` | F00, F02 | M |
| F11 | [End-to-end demonstration](f11-e2e.md) | integration tests and fixtures | thin test after F00; full test after F03–F10 | L |

Sizes are relative planning aids, not time estimates. Each feature has one owning agent. Agents may develop in separate worktrees and integrate through reviewed commits.

## Parallel sequence

1. **Contract gate:** F00 establishes the core API, ownership boundaries, event schema, database migration convention, and versioned MCP namespace. Keep this gate small; do not implement the feature modules inside it.
2. **First parallel wave:** F01, F02, F03, F08, F09, and F10 can start. F08 and F09 implement their adapters against the agreed API and wire in operations as they arrive. F10 can define rendering and fixtures against the event contract while F02 implements storage.
3. **Core wave:** F04 and F05 start when F02 exposes transactional storage. F06 and F07 start after F04 exposes task transitions. F11 grows continuously from a minimal real MCP session to full scenarios.
4. **Integration gate:** F03–F10 are connected through the same core operations; F11 passes against real local Git repositories and MCP sessions.

The critical path is F00 → F02 → F04 → F06 → F11. Add agents to independent lanes before adding them to that path.

## Interface and change rules

- F00 owns shared types, core operation signatures, `Cargo.toml`, and crate wiring. Other owners propose contract changes to F00's owner rather than editing those files concurrently.
- F02 owns schema and migrations. Other owners state storage needs in their feature specs or an interface request; they do not add their own tables independently.
- The core owns authorization, policy checks, and state transitions. MCP and CLI are adapters; neither duplicates transition rules.
- A state change and its event are committed in one SQLite transaction. Mutating operations accept an idempotency key and return the resulting object and event ID.
- Git side effects cannot share a SQLite transaction. F00 and F03 must agree on a recoverable operation record and reconciliation behavior before Git mutation code is integrated.
- Use the installed `git` executable and the dependencies selected in the product spec. Keep the MVP in one crate.

## Milestone exit

F11 must demonstrate every acceptance criterion in [spec §11](../../spec.md#11-mvp-acceptance-criteria), including two repositories, concurrent coding agents, two independent reviewers, a revision round, restart recovery, autonomous completion, a limit outcome, a human gate, portable exports, and dirty-worktree cleanup refusal. Record the commands and observed results in the F11 test documentation. A completed run identifies reviewed commits and remaining integration steps; it does not merge them.
