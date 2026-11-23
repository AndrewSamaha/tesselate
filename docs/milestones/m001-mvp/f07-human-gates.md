# F07: Optional human decisions

**Owner:** `coordination::human` agent. **Depends on:** F04. **Output:** durable policy-governed requests and resolutions.

## Scope

- Evaluate configured human gates before affected transitions, such as accepting a deferred high-severity finding or completing a run.
- Store each request's question, context, choices, requesting task, expiration/escalation policy, status, and resolution.
- Pause only the governed transition. Other ready tasks and assignments continue.
- Resolve a request through an authorized CLI operation, record actor/reason/event, and allow the blocked transition to resume when policy permits.
- Record expiration or escalation outcome rather than leaving an indefinite, invisible wait.

## Acceptance

- With no gates configured, a run can complete without a human response.
- A configured gate survives restart, pauses only its transition, and resumes after CLI resolution.
- Invalid choices, duplicate or conflicting resolutions, and unauthorized actors are rejected or handled idempotently under the F00 contract.

**Boundary:** CLI display and input belong to F09; this module owns decision state and policy.
