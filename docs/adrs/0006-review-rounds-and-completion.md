# ADR-0006: Durable review rounds and task completion

## Status

Accepted for M001 F06.

## Decision

Review rounds are durable records separate from reviewer results. Each round stores the task, monotonically numbered round, submitted repository/commit refs, and prior finding IDs. The round-list read returns pending rounds as well as rounds with submitted reviews, so clients can resume after restart without inferring requests from results.

Review requests must reference the task's persisted submitted commits. F03 remains responsible for proving that those refs belong to the associated workspaces and repositories; F06 only checks project membership and matches the task submission. A reviewer cannot be an author of any submitted commit. The validated project policy requires at least two independent reviewers. Findings require severity, location, evidence, and proposed correction, and every finding requires a persisted supervisor disposition before completion. Findings marked fixed must appear in a later review round whose refs differ from the source round.

Review submission and disposition update the affected round revision and task revision fence in the same F02 mutation as their records and event. The F06 completion gate returns compare-only revision guards for the rounds it evaluated. F04 commits those guards atomically with the task's transition to Completed. This serializes review writes against completion without moving task transition policy into F06.

## Consequences

An empty/pending round is queryable before any reviewer responds. Concurrent review or disposition writes cannot be omitted from a completion decision, and late results cannot modify a completed task. Policy lookup, author lookup, human deferral approval, task transitions, and Git ownership remain narrow cross-feature interfaces.
