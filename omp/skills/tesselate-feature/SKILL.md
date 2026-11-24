---
name: tesselate-feature
description: Start or resume a reviewed local feature using Tesselate, separate OMP processes, and an integration worktree.
---

# Tesselate feature

Open an interactive OMP session from `~/Development` (select a local repo) or inside a repository beneath that workspace root. Request a feature naturally (for example, “Build a feature for this repo that …”), or use `/feature <goal>`. The extension collects acceptance criteria and constraints, then asks OMP to draft a specification. Clarify ambiguities before calling `tesselate_feature_start`. Confirm scope, the local base branch, and an explicit integration-only validation command. Do not code in the initiating session.

If the user leaves the validation input blank, use the agent's proposed command only after showing it in a separate confirmation. If no command was proposed, inspect repository test instructions and retry the tool call; never start a run without an approved validation command.

Tesselate registers the canonical repository, creates a run and task, assigns a coder credential, and provisions a coder worktree pinned to the selected integration base. Separate `omp -p` processes implement and review; two different non-author reviewers use detached read-only checkouts. Reviews and revisions are recorded against captured commit refs, not a worker's self-reported success. Only accepted commits fast-forward onto the dedicated integration branch. Validation runs only there; the extension exports run artifacts. No automatic push, PR creation, or merge into `main`.

The per-run handoff, worker session directories, process/log handles, and error are private under `<state_dir>/omp-runs/<run_id>/`. A pre-run intent is written before run creation so a crash can replay the same idempotency key. If interrupted, use `/feature resume` from the original repo (or select that repo from the workspace root); optionally append the full run UUID to target it directly. Resume requires no new specification or validation command. Inspect any reported mismatch before proceeding. Failed/interrupted integration validation requires an explicit inspected-log retry. Do not delete dirty worktrees to repair a failed run. Processes running under the same OS user are not a security boundary; role credential files prevent accidental authority mixing, not malicious local-file access.

Read the repository's AGENTS.md. For Bitwars, never merge into local `main` or start app/server/reset Redis in task worktrees. If a Tesselate per-repository provisioning policy exists, show the exact ignored paths for approval before copying them to the integration worktree only; absent optional paths are skipped, and coder/reviewer checkouts receive none. Do not execute workmux hooks or symlink ignored files. Generic repositories also require an explicit local base branch and a user-approved validation command. Never assume a package install or shared-service test is safe. If review, validation, provisioning, or a worktree identity check cannot be completed, leave the branch/run for recovery and report the exact obstacle.
