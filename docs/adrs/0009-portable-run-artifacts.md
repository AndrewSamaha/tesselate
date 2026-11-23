# ADR-0009: Portable run artifacts

Status: Accepted for M001

F10 exports a run snapshot under the F01 artifact directory using stable names: `events.v1.jsonl`, `records.v1.jsonl`, and `timeline.v1.md`. The event log is paged from F02 in ascending immutable sequence. A second versioned JSONL carries current run-scoped durable records that events reference but intentionally do not duplicate, such as message bodies and persisted review findings. The Markdown timeline links those identifiers and includes the related records so readers do not need SQLite or a running coordinator.

The exporter reads only the scoped event and entity APIs. It does not add events or change workflow state. Its three paths are deterministic and exports can be repeated after restart. Each file is written to a temporary sibling, synced, and renamed into place. A crash can leave artifacts from different attempts, but rerunning replaces all three from the current persisted snapshot.

Credential issuance event types are omitted. Fields whose keys name secrets, credentials, passwords, tokens, verifiers, authorization data, API keys, private keys, or machine-local filesystem paths are recursively omitted from event and related-record payloads. Producers must keep credentials, secret configuration, and complete repository file contents out of persistent history. Markdown escapes all untrusted text and creates local links only for validated UUIDs and hexadecimal commit IDs. F09 invokes export; F08 owns authorization and discovery through the existing artifact-location contract.
