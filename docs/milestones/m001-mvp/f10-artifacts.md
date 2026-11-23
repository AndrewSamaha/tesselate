# F10: Portable run artifacts

**Owner:** `artifacts` agent. **Depends on:** F00, F02. **Output:** repeatable JSON Lines and Markdown timelines.

## Scope

- Export the immutable event log as versioned JSON Lines with stable ordering and enough actor, task, repository, commit, review, decision, and outcome references to reconstruct a run.
- Render a Markdown timeline from the same stored events, linking related tasks, review rounds, and Git references.
- Make repeated export after a crash deterministic and leave authoritative history unchanged. Avoid credentials, secret configuration, and unnecessary full file contents.
- Expose export through F09 and artifact discovery through F08.

## Acceptance

- Repeated export of unchanged history produces equivalent content.
- A standalone reader can follow assignments, messages, submissions, reviews, revisions, dispositions, and final outcome without SQLite or Tesselate running.
- Tests cover escaping, ordering, incomplete runs, and secret-field exclusion.

**Boundary:** this module reads history; it never invents events or decides workflow outcomes.
