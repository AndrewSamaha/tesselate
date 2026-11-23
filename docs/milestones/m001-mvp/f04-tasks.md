# F04: Runs, tasks, and assignments

**Owner:** `coordination::tasks` agent. **Depends on:** F00, F02. **Output:** transactional task lifecycle and scheduling rules.

## Scope

- Create runs and tasks, record task goals and dependencies, and expose eligible work. A task becomes ready only when required predecessors complete.
- Implement the specified states: `ready`, `assigned`, `coding`, `submitted`, `in_review`, `revision_required`, `completed`, `blocked`, `failed`, and `cancelled`.
- Enforce actor permissions and legal transitions; record actor, reason, and event ID for each transition.
- Assign/claim/release work, update progress, submit commit references, and record final disposition. Integrate F03 workspace requests without duplicating Git logic.
- Enforce maximum concurrent assignments, task retries, and run duration. A reached limit produces a durable terminal outcome identifying remaining work and reason.
- Make expired assignments recoverable and reassignable without discarding worktrees or history, using F05 lease state.

## Acceptance

- Transition tests cover legal/illegal moves, dependency release, role checks, retry and concurrency limits, and duplicate calls.
- A failed Git operation leaves task state unchanged or in the documented recoverable checkpoint state.
- Limit outcomes and reassignment remain readable after reopening SQLite.

**Boundary:** review satisfaction belongs to F06; human-policy pauses belong to F07. This module invokes those checks before completion.
