# ADR-0005: Task lifecycle and workflow limits

## Status

Accepted for M001 F04.

## Decision

`coordination::tasks::TaskEngine` owns authorization and legal transitions for runs, tasks, and assignments. Every successful transition writes the affected records and one event through F02 `Database::commit_mutation`; run/task revisions provide compare-and-swap protection, while the database transaction remains the atomicity boundary. Idempotency is looked up after authentication and before policy validation, and rechecked after a state conflict so a concurrent identical request returns its original result.

A run snapshots validated project workflow limits when it is created. Duration expiry and retry exhaustion stop the run durably and record a reason plus the remaining task IDs. Assignment concurrency is checked against all active assignments in the run, not only the current result page. Assignment history is append-preserving: an expired owner is marked superseded and a new assignment record is added; F04 does not delete workspaces or Git history.

Dependencies are stored with task records and mirrored to the F02 dependency index. Completing a prerequisite updates all newly unblocked dependent tasks in the same mutation transaction. A task may complete only after submission and the review gate; F06 supplies review satisfaction, F07 supplies human pauses, and F05 supplies lease expiration.

## Consequences

The engine returns recoverable errors for concurrency and policy limits after persisting any terminal run outcome. Review, human approval, session leasing, workspace/Git work, and external artifact management remain separate feature boundaries.
