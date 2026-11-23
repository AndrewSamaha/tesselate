# ADR-0008: Validated project configuration

Status: Accepted for M001

Project TOML is deserialized with unknown fields rejected at the project, repository, workflow, review, and human-gate levels. Safety-sensitive policy must not be silently ignored when a spelling or version is unknown. Workflow limits must be positive and the project review policy must require at least two independent reviewers.

Repository IDs are stable nonempty ASCII slugs. Repository paths resolve relative to the configuration file, are canonicalized, and must name distinct Git working-tree roots. The artifact destination resolves relative to that same base; it may not exist yet, but its nearest existing directory must be writable. Validation uses a uniquely named temporary file and removes it immediately, without creating the requested artifact directory.

The validated core object stores paths as canonical strings and preserves an absent human-gate table as `None`. An explicitly configured table remains `Some`, even when every configured gate is disabled, so registration and policy code can distinguish omission from explicit policy. `completion_approval` and `high_severity_deferral_approval` default to false when omitted from an existing `[human]` table. The latter is passed to F06 as `high_severity_defer_requires_human`; an absent or disabled gate permits autonomous progress. F01 stops at validation; project persistence and CLI parsing remain F02 and F09 responsibilities.
