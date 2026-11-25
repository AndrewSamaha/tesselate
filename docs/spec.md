# Tesselate: Product and Technical Specification

Status: Draft 0.1  
Date: 2026-11-22

## 1. Purpose

Tesselate is a local coordination layer for coding agents. It exposes tools through the Model Context Protocol (MCP) to agent harnesses such as oh-my-pi, coordinates agents working across multiple on-machine Git repositories, and records how work and decisions unfolded. Its primary use case is a long-running workflow that completes without human intervention. Projects may also configure human decision and approval points.

Tesselate coordinates work; the agent harness runs the agents. The MVP may use a test harness to start agents, but Tesselate does not need to launch or host models.

## 2. Goals and non-goals

### Goals

- Run as a headless local service with a small, scriptable CLI.
- Serve multiple concurrent agent sessions through MCP.
- Coordinate a supervisor, coding agents, and independent adversarial reviewers.
- Give coding agents isolated Git worktrees in one or more registered repositories.
- Continue or recover long-running work after agent disconnects and service restarts.
- Support fully autonomous workflows and optional human review policies.
- Produce portable artifacts that reveal assignments, communication, code references, reviews, decisions, and outcomes.
- Keep the core modular and provide defined extension points without requiring plugins for the MVP.

### Non-goals for the MVP

- Hosting models or replacing an agent harness.
- Cloud coordination, remote repositories, or multi-host worktrees.
- Automatically merging changes into a repository's primary branch.
- Automatically deleting branches, worktrees, or uncommitted changes.
- A graphical user interface.

## 3. Concepts and roles

| Concept | Meaning |
| --- | --- |
| Project | A coordination boundary containing repositories, tasks, agents, policies, and history. |
| Repository | An existing local Git repository registered with a project. |
| Run | One attempt to fulfill a project goal under configured limits. |
| Task | A unit of work with a goal, owner, dependencies, status, and result. |
| Agent session | One connected agent identity, role, and lease. |
| Assignment | A task given to an agent, possibly with worktrees in several repositories. |
| Review | An independent assessment of submitted changes, with findings or an explicit pass. |
| Human request | A persisted decision or approval request that pauses only the work governed by its policy. |
| Event | An immutable record of a state change or consequential communication. |

A **supervisor agent** decomposes the goal, assigns tasks, tracks dependencies, sends work for review, routes findings for revision, and records the final disposition. **Coding agents** implement assigned tasks in their worktrees. **Review agents** look for defects, unmet requirements, unsafe behavior, and integration problems. A review agent must not review changes it authored. The coordinator enforces that separation; the review brief asks the reviewer to challenge the implementation and cite evidence for each finding.

## 4. Architecture and technology

Tesselate is written in Rust. The initial implementation is one crate with separate modules for coordination, MCP, Git, persistence, configuration, artifacts, and CLI. Modules may become crates when an actual reuse or build boundary requires it.

| Component | Initial choice |
| --- | --- |
| MCP interface | `rmcp` Rust SDK |
| Async service and local connections | Tokio |
| CLI | `clap` |
| Durable state | SQLite through `rusqlite`, with write-ahead logging and transactions |
| Configuration | TOML parsed with `serde` and `toml` |
| Git operations | The installed `git` executable, invoked with explicit arguments |
| Diagnostics | `tracing` |

One local coordinator process owns the database. A small stdio MCP adapter connects each harness session to it over a permission-restricted Unix socket. This permits several stdio-based agents to share state without racing over separate service instances. The core API remains independent of transport so another MCP transport can be added later.

The core owns state transitions and policy checks. MCP and CLI are adapters over the same core operations. Extensions may register additional commands, tools, or artifact exporters through versioned interfaces; the MVP needs only built-in implementations.

## 5. Workflow

### 5.1 Task and review lifecycle

A task moves through `ready`, `assigned`, `coding`, `submitted`, `in_review`, `revision_required`, and `completed`. It can also end as `blocked`, `failed`, or `cancelled`. Every transition records its actor, reason, and event ID. Dependencies prevent a task from becoming ready until its required predecessors complete.

The supervisor submits a review request referencing the task goal, relevant repository and commit IDs, and any previous findings. A reviewer returns an explicit pass or findings with severity, location, evidence, and a proposed correction. The supervisor must disposition each finding as fixed, accepted with reason, or deferred with reason. A revised submission starts a new review round; previous rounds remain in the history.

Completion requires the project's review policy to be satisfied. For the MVP demonstration, at least two reviewers who did not author the submitted changes must finish their reviews. An explicit pass is a recorded review result, not an absence of findings.

### 5.2 Autonomous execution

The default workflow requires no human response. The supervisor may assign work, request reviews, send findings back, and complete or stop a run within configured limits. Limits include wall-clock duration, review/revision rounds, task retry count, and concurrent assignments. Reaching a limit produces a recorded terminal outcome with the remaining work and reason.

Agent sessions renew leases while active. When a lease expires, the task becomes recoverable; the coordinator retains its worktree and history and permits a supervisor to resume or reassign it. State survives coordinator restarts. Agent messages and task changes must be queryable after reconnection. The MVP may use polling for new assignments and messages.

### 5.3 Optional human input

Project policy may require a human decision before specified transitions, such as accepting a deferred high-severity finding or completing a run. A human request contains the question, context, choices, requesting task, and expiration or escalation policy. It is stored durably and resolved through the CLI. Only the affected transition pauses; other eligible work may continue. A project with no human gates must be able to finish entirely unattended.

## 6. Git and worktree behavior

A project registers one or more existing local Git repositories. For each repository an assignment touches, Tesselate creates an isolated branch and worktree and records the canonical repository path, base commit, branch, worktree path, owner, and resulting commit IDs. Worktree creation is idempotent for a given assignment and repository.

Before a Git mutation, Tesselate verifies the target repository, branch, and worktree state. It does not overwrite an existing path or branch. Cleanup is an explicit CLI operation that refuses a dirty worktree unless the human supplies a separate, deliberate force action. The MVP leaves integration and merges to the user or an external process; a completed run identifies reviewed commits and any unresolved integration steps.

## 7. MCP and CLI surface

The initial MCP tools cover:

- **Discovery:** list projects, repositories, agents, runs, tasks, and available work.
- **Coordination:** create and assign tasks, claim or release an assignment, update progress, submit work, record a final disposition, and reopen a budget-exhausted task with a supervisor repair.
- **Communication:** send task-scoped or project-scoped messages and read messages since an event cursor.
- **Workspace:** request or inspect assignment worktrees and their Git references.
- **Review:** request a review, submit findings or a pass, and read review rounds.
- **Human input:** create a policy-governed request and read its resolution.
- **History:** read a bounded task or run timeline and locate exported artifacts.

Mutating calls accept an idempotency key and return the resulting object and event ID. The core checks each caller's session and role before changing state. The exact tool names and JSON schemas will be specified during implementation, with a stable versioned namespace.

The CLI initially supports starting the service, registering a project or repository, inspecting runs and tasks, viewing pending human requests, resolving a request, exporting a timeline, and inspecting or cleaning up worktrees. Read commands support machine-readable JSON output. CLI and MCP calls apply the same transition rules.

## 8. Configuration

Configuration is TOML. Project files declare repository paths, artifact destination, workflow limits, review policy, and optional human gates. Paths are resolved and validated when the project is registered. The parser rejects unknown safety-critical policy keys instead of silently ignoring them.

Illustrative shape, subject to validation during implementation:

```toml
name = "example-project"
artifact_dir = ".tesselate/artifacts"

[[repositories]]
id = "api"
path = "/path/to/api"

[[repositories]]
id = "web"
path = "/path/to/web"

[workflow]
max_duration_hours = 24
max_concurrent_assignments = 4
max_task_retries = 2
max_review_rounds = 3

[review]
min_independent_reviewers = 2

[human]
completion_approval = false
high_severity_deferral_approval = false
```

## 9. State and artifacts

SQLite is the authoritative store for task state, leases, messages, reviews, human requests, and an append-only event log. Each state change and its event are committed in one transaction. The event log exports as versioned JSON Lines. A generated Markdown timeline summarizes the same events and links to task, review, and Git references. Export can be repeated after a crash without changing the underlying history.

Artifacts must identify who acted, when, on which task and repository, what changed, and why. They include agent-to-agent messages and review dispositions, while avoiding secrets and unnecessary full file contents. The output is readable without Tesselate and remains useful if the database is unavailable.

## 10. Reliability and local security

- Only registered local repositories and validated worktree paths may be modified.
- The Unix socket and state directory are accessible only to the local user by default.
- Agent sessions receive coordinator-issued credentials; a client cannot gain supervisor or reviewer authority by asserting a role in a tool argument.
- Long-running operations record checkpoints and terminal reasons. Reconnection and service restart do not erase pending work or human requests.
- Git failures return actionable errors and leave the previous task state intact where possible.
- Logs and artifacts omit credentials and secret configuration values.

## 11. MVP acceptance criteria

1. A supervisor agent divides a goal involving two registered repositories into tasks and assigns them.
2. At least two coding agents work concurrently in separate worktrees and submit commits.
3. At least two independent review agents perform adversarial reviews of the submitted changes, recording concrete findings or explicit passes.
4. The supervisor routes findings to coding agents, receives revisions where required, and starts another review round.
5. The workflow successfully completes without human input, within configured limits. A separate limit test records why an unfinished run stopped.
6. Restarting the coordinator during the run preserves assignments, messages, worktree references, pending reviews, and usable history.
7. Exported JSON Lines and Markdown let a person reconstruct assignments, communication, code references, reviews, revisions, and the final decision without running Tesselate.
8. In a separate configured run, a human gate pauses the affected transition, accepts a CLI resolution, and allows the workflow to continue.
9. Worktree cleanup refuses to discard uncommitted changes through the ordinary cleanup command.

The end-to-end demonstration should use real local Git repositories and agent sessions connected through MCP, rather than calling the core directly. The harness may supply the agent processes. It must exercise the supervisor, coding, and review roles and the autonomous path.
