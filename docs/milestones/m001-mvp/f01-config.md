# F01: Project configuration

**Owner:** `config` agent. **Depends on:** F00. **Output:** validated project configuration for registration and startup.

## Scope

- Parse TOML for project name, repository IDs and paths, artifact destination, workflow limits, minimum independent reviewers, and optional human gates.
- Resolve and validate paths at registration, including canonical repository paths and an artifact destination the local user can write.
- Reject unknown safety-critical policy keys and invalid values with actionable errors. Preserve the distinction between an absent human gate and a configured one.
- Expose validated configuration through F00's core types. Work with the CLI owner on registration input and error presentation.

## Acceptance

- Valid multi-repository configuration registers with resolved paths.
- Tests reject duplicate repository IDs, nonexistent or non-Git repository paths, invalid or zero limits, invalid review policy, and unknown policy keys.
- A project without human gates remains eligible for fully autonomous completion.

**Boundary:** parsing and validation live here; project persistence and CLI argument handling belong to F02 and F09.
