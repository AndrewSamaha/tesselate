# F06: Independent review workflow

**Owner:** `coordination::reviews` agent. **Depends on:** F04. **Output:** review rounds and completion policy.

## Scope

- Create review requests containing task goal, submitted repository/commit IDs, and prior findings when revising.
- Record each reviewer's explicit pass or findings with severity, location, evidence, and proposed correction. Reject a reviewer who authored any submitted change being reviewed.
- Persist rounds separately. Require the supervisor to disposition every finding as fixed, accepted with reason, or deferred with reason; a revised submission starts a new round without rewriting earlier results.
- Enforce the configured minimum independent reviewer count and any policy conditions before task completion. For the MVP demonstration, require at least two non-author reviewers.
- Coordinate high-severity deferral checks with F07 when project policy requires human input.

## Acceptance

- Tests reject self-review, missing explicit results, insufficient reviewers, and undispositioned findings.
- A finding, revision, and second review round remain distinct and queryable after restart.
- A task can complete only when the review policy is satisfied; a pass is recorded as a result, not inferred from silence.

**Boundary:** this module determines review eligibility and satisfaction; F04 performs task transitions and F08 exposes the tools.
