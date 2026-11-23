# F00: Crate and shared contracts

**Owner:** one foundation agent. **Dependency:** none. **Output:** a compiling Rust crate and stable interfaces that other agents can implement against.

## Scope

- Create the single-crate layout: `coordination`, `persistence`, `git`, `mcp`, `config`, `artifacts`, and `cli`. Keep feature modules as empty or minimal stubs until their owners implement them.
- Define IDs, roles, statuses, actor identity, error types, and core request/result types. Core operations accept an authenticated session context; role is never taken from a mutation argument.
- Define operations for project/run/task discovery, task assignment and progress, messages, workspace requests, reviews, human requests, and timelines. Preserve a transport-independent core API used by MCP and CLI.
- Define a versioned event envelope with event ID, timestamp, actor, project/run/task scope where applicable, event type, and structured payload. Define pagination and event-cursor semantics.
- Define idempotency scope and result behavior for repeated mutating calls, plus the contract for transactional event writes.
- Choose the credential issuance and bootstrap flow for local supervisor, coder, reviewer, and CLI sessions. Document how credentials are stored and revoked.
- Define a durable Git operation intent/checkpoint and reconciliation contract for a crash between Git and SQLite steps. Specify what a retry does when a branch or path already exists.
- Resolve the completion meaning: a task/run may be completed after policy-satisfying reviews, while integration remains external; results must list reviewed commit IDs and outstanding integration steps.
- Define the versioned MCP tool namespace and JSON request/response schemas, including errors. Exact names may be set here, before the MCP agent implements handlers.

## Acceptance

- `cargo check` passes with the module skeleton.
- Contract tests cover serialized event and MCP shapes, role/context boundaries, and idempotency result semantics.
- A short contract document records the three decisions above and gives F01–F11 a stable implementation target.
- No feature-specific behavior is implemented merely to make an interface look complete.
