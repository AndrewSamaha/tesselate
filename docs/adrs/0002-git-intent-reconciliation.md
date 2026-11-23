# ADR-0002: Recoverable Git mutation intent

Status: Accepted for M001

Before invoking Git, persist an intent keyed by assignment and repository, including canonical repository and worktree paths, target branch, base commit, owner, and `prepared` checkpoint. Advance to `mutation_started`, perform Git worktree creation from the exact base commit, persist observed repository path/worktree path/branch/HEAD/cleanliness as available, and advance to `git_applied` then `recorded`, with the resulting `Workspace` attached. `conflict` records an ambiguous or mismatched observation and a recovery reason. Never overwrite an existing branch or path. On retry, inspect both Git metadata and filesystem: record success only when branch, worktree path, repository, and HEAD match this intent; report conflict for mismatches or ambiguous partial state. If no side effect exists, retry the same intent. If the matching worktree exists but DB recording was interrupted, reconcile and record it. Do not automatically remove partial or conflicting resources; explicit cleanup owns deletion.

SQLite and Git cannot share a transaction. This intent protocol makes the gap inspectable and retryable without claiming atomicity. F03 owns its concrete command sequence and reconciliation tests; F02 persists the checkpoint.
