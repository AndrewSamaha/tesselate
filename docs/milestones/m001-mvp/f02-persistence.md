# F02: Durable state and events

**Owner:** `persistence` agent. **Depends on:** F00. **Output:** SQLite storage behind the agreed core storage interface.

## Scope

- Create schema and migrations for projects, repositories, runs, tasks, dependencies, assignments, sessions/leases, messages, worktree and commit references, reviews/rounds/findings/dispositions, human requests, idempotency records, Git operation checkpoints, and immutable events.
- Enable WAL mode, foreign keys, and appropriate busy handling. One coordinator process owns writes.
- Provide transactional methods so every state mutation and its event commit together. A repeated idempotency key in its defined scope returns the prior result without duplicating effects.
- Provide bounded reads for task/run history and messages after an event cursor. Persist enough state for restart and expired-lease recovery.
- Own migration tests and storage fixtures; expose needed operations to F04–F07 and F10 without leaking SQL into those modules.

## Acceptance

- Tests show atomic state/event writes, including rollback on error, and stable results for repeated mutation keys.
- Closing and reopening the database preserves assignments, messages, reviews, human requests, Git references, and event order.
- Concurrent service tasks can read and serialize writes without corrupting state or producing duplicate events.
- Migration from an empty database works deterministically and reports an incompatible schema clearly.

**Boundary:** this module stores state; coordination modules decide whether a transition is allowed.
