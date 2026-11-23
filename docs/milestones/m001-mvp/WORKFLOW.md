# M001 development workflow

This is the process for building Tesselate. The [product spec](../../spec.md) separately says that the Tesselate MVP does not automatically merge changes in projects it coordinates. Here, the engineering lead merges completed **Tesselate implementation features** into this repository's `main` branch.

## Roles and ownership

- **Engineering lead:** owns the milestone plan, assignment order, interface decisions, final feature evaluation, integration, and merges to `main`. The lead watches dependencies and agent capacity, and starts newly unblocked features while other work continues.
- **Coding expert:** owns one assigned feature and its branch/worktree, implementation, relevant tests, and responses to findings. An agent may take another feature after its current work is handed off, but each feature has one accountable coding owner at a time.
- **Reviewer:** independently examines the feature's goals, code, tests, and integration risks. A reviewer does not review their own code and does not merge the branch. Additional specialist reviews may be requested where the change warrants them.

The lead may assign several independent features concurrently according to the [delivery map](README.md#delivery-map). F00's shared contracts and F02's migrations have designated owners; other agents request changes to those surfaces rather than editing them in parallel.

## Per-feature cycle

1. **Assign.** The lead gives the coding expert a feature spec, current dependencies and interfaces, expected acceptance checks, and any file ownership constraints. A feature starts only when its stated prerequisites are available. The lead records blockers and keeps other eligible work moving.
2. **Create a workspace.** The coding expert creates one feature branch from the current `main` and a dedicated Git worktree for it. Branch names use `m001/fNN-short-name`. They work and commit only in that worktree. If `main` advances, the branch incorporates the new base before final review when the change can affect this feature.
3. **Implement and self-check.** The coding expert makes focused commits, runs relevant formatting, build, and tests, and checks the feature's acceptance criteria. They avoid unrelated changes and report any contract change or blocker to the lead promptly.
4. **Hand off for review.** The coding expert signals readiness with the branch and commit ID, a concise change summary, acceptance evidence, known limitations, and any affected interfaces. The lead assigns an independent reviewer.
5. **Review and revise.** The reviewer checks behavior, code clarity, consistency with the project, tests, and effects on adjacent modules. Findings cite a concrete location and consequence; a pass is explicit. The coding expert fixes findings or explains a proposed disposition and commits revisions. The reviewer checks the revised commits. Repeat until the reviewer passes or the lead resolves a disputed finding.
6. **Lead evaluation.** After review passes, the lead evaluates the final diff against the feature spec and the project's longer-term goals: architecture fit, API consistency, clarity, duplication, maintainability, quality, and integration with already merged work. The lead runs or verifies the relevant checks. Issues go back to the coding expert, followed by reviewer re-check when code or behavior materially changes.
7. **Merge.** Once the lead accepts the feature, they bring it up to date with `main`, resolve integration conflicts with the coding expert as needed, verify the final result, and merge it into `main`. A material change during integration returns to review. The lead records the merged commit and marks the feature complete only after confirming it is on `main` and its acceptance checks still pass.
8. **Unblock next work.** The lead updates the dependency map and assigns newly ready features to available coding experts. Branch and worktree cleanup is a separate explicit step after the merge and after checking for uncommitted work.

## Gates and exceptional paths

| State | Exit condition |
| --- | --- |
| Blocked | A named dependency, interface decision, or external issue is resolved; the lead confirms the feature can start or resume. |
| In progress | Coding expert provides a committed review handoff with acceptance evidence. |
| In review | Reviewer explicitly passes the current revision, with findings resolved or dispositioned. |
| Lead evaluation | Lead accepts the feature's code and fit, and relevant checks pass. |
| Complete | The reviewed feature is merged into `main` and verified there. |

- Review approval applies to the commits reviewed. Substantive changes after that point need another review.
- If a feature's dependency changes incompatibly, the lead coordinates the affected owners and updates the contract or feature spec before they continue.
- If a reviewer and coder disagree, the lead decides the disposition and records why. A decision does not waive an acceptance criterion without updating the spec.
- If a feature cannot be completed, the lead records the blocker and its effect on dependent features, then reassigns or reschedules work. No agent silently skips a required acceptance check.
- Parallel branches are integrated one at a time. The lead checks each against current `main` so a previously passing branch does not bypass conflicts or new failures.

## Milestone completion

Every F00–F11 feature must be complete on `main`. The lead then runs the full F11 demonstration against the integrated code and confirms all nine MVP acceptance criteria in the [product spec](../../spec.md#11-mvp-acceptance-criteria). The milestone is done only when the integrated result passes and any remaining limitations are documented.
