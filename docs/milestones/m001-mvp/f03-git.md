# F03: Git repositories and workspaces

**Owner:** `git` agent. **Depends on:** F00. **Output:** safe, recoverable local Git operations for assignments.

## Scope

- Verify registered local repositories and base commits before mutation. Invoke installed `git` with explicit arguments.
- For each assignment/repository pair, create and record an isolated branch and worktree with canonical repository path, base commit, branch, path, owner, and resulting commit IDs.
- Make creation idempotent. Detect conflicts with existing paths or branches without overwriting them.
- Implement the F00 intent/checkpoint reconciliation contract. A restart or retry must distinguish Tesselate's own completed operation from an unrelated existing branch/path and report ambiguous state for human inspection.
- Inspect worktree status and implement explicit cleanup used by the CLI. Ordinary cleanup refuses dirty worktrees; any force action is separate and deliberate.

## Acceptance

- Tests with temporary real repositories cover two repositories in one assignment, repeat creation, conflicting branch/path, and commit reference capture.
- A simulated interruption between Git and database updates reconciles to one recorded worktree or a clear recoverable error.
- Ordinary cleanup refuses uncommitted changes and never deletes a branch or worktree without the explicit cleanup command.

**Boundary:** no merges into a repository's primary branch and no automatic cleanup at run completion.
