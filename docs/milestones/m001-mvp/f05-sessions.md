# F05: Agent sessions, leases, and messages

**Owner:** `coordination::sessions` agent. **Depends on:** F00, F02. **Output:** authenticated, recoverable agent participation.

## Scope

- Issue or validate coordinator credentials through F00's bootstrap contract; bind identity and role to a session, rather than trusting a tool argument.
- Open, renew, expire, and reconnect sessions. On expiry, expose affected assignments for supervisor recovery while retaining worktree references and history.
- Persist task-scoped and project-scoped messages with sender, timestamp, scope, and event ID. Read messages after a cursor with bounded pagination.
- Keep stored and exported diagnostics free of credentials and secret configuration values.

## Acceptance

- Tests reject forged role claims and unauthorized mutations.
- Lease expiry makes an assignment recoverable; reconnect or restart preserves its messages and references.
- A client reading after an event cursor receives each later message in order, without duplicates.

**Boundary:** socket connections and MCP tool handlers belong to F08; this module owns session semantics.
