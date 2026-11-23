# ADR-0007: Credentials, sessions, and assignment leases

## Status

Accepted for M001 F05.

## Decision

The coordinator generates high-entropy bearer credentials. SQLite stores only a SHA-256 verifier digest in the `credentials` entity; the bearer secret is returned once and is not serializable or logged by `IssuedCredential` debug output. An exact issuance retry returns the original session and actor IDs with `secret: None` and `replayed: true`; plaintext is never persisted to make a retry recover it. This also applies when concurrent identical issuance calls race and only one transaction wins. The bootstrap verifier is single-use and accepts no role claim. It creates the first supervisor; later coder, reviewer, and CLI credentials require a live supervisor context and bind a durable actor and role. Authentication reloads credential state and creates an opaque `SessionContext` only for an active, unexpired record. Revoke and explicit expiry mark the credential unusable and expire any leases it owns.

Each F04 assignment has a corresponding F05 `AssignmentLease` entity keyed by the assignment record ID. A successful agent renewal creates or increments its generation and expiry, while checking that the F04 assignment revision is still current in the same F02 mutation. F04 reassignment checks the lease entity revision as a compare-only precondition. If renewal wins, reassignment must re-read and refuse or retry; if reassignment wins, renewal's assignment precondition fails. A lease begins at the assignee's first renewal; an assignment with no lease cannot be reclaimed as expired.

Messages are persisted as scoped entities and associated with append-only events. Event payloads identify the message and sender without copying the message body; paginated reads use F02 sequence-backed cursors. Default session lifetime is 12 hours and assignment lease lifetime is two minutes. F08/F09 own local secret presentation and permission-restricted transport/files.

## Consequences

Credentials remain verifiable after restart without storing plaintext bearer material. Lease generation and F02 revisions fence renewal against reassignment without duplicating F04 task-transition policy. Assignments that have never completed a renewal remain assigned until a valid agent session opens or a supervisor handles recovery. Adding these entities upgrades schema v1 to v2.
