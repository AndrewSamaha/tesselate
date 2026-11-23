# ADR-0004: SQLite transaction and storage boundary

Status: Accepted for M001

`persistence::Database::commit_mutation` is the one durable mutation boundary. It uses an immediate SQLite transaction and accepts a typed `StateChange<T>` entity payload, expected revision, canonical request/result, idempotency scope, and event draft. The database checks the state precondition, applies the state write, appends the immutable event with its monotonic sequence, links the entity to the event, and records the idempotency result before committing. Replays return the stored result and event ID without applying a second state change; reusing a scoped key for another request is an error. The coordinator owns authorization and transition policy and supplies only already-authorized state changes.

SQLite uses WAL, foreign keys, a five-second busy timeout, and serialized writes through the owning `Database` handle. Event cursors are opaque random tokens persisted with their sequence position, so a client cannot invent a valid cursor by changing the sequence. The versioned migration is deterministic and rejects a database created by a newer schema. `load`, `list_entities`, `events_after`, and `messages_after` keep reads behind the persistence boundary.
