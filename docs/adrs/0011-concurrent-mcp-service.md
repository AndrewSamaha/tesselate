# ADR-0011: Concurrent local MCP service

Status: Accepted for M001

## Decision

One Tokio coordinator owns the SQLite-backed `Coordination` service. It listens on `coordinator.sock` inside a dedicated state directory with mode `0700`; the socket and advisory process-lock file use `0600`. An exclusive `flock` prevents a second coordinator from claiming the same socket, and allows a later process to remove a stale socket left after a crash. The coordinator handles independent connections concurrently and bounds each newline-delimited internal request frame to 1 MiB.

Each harness starts the small `tesselate-mcp` process. It serves the versioned F00 tools through `rmcp` stdio and forwards one request per local socket connection. Its bearer credential is injected by the local launcher, never accepted in tool arguments, and sent only inside the private socket envelope. The coordinator authenticates each request through F05 and passes the opaque `SessionContext` to the transport-neutral `Coordination` API. The adapter carries the canonical idempotency key and serializes the core result, including event IDs and replay markers, without implementing role or transition policy.

Caller-initiated mutations require an idempotency key at both the tool schema and coordinator boundary. `available_work` and human-request listing remain discovery operations; their deterministic expiry materialization is internal lifecycle work using F04/F07's existing durable event and idempotency behavior, so repeated polling does not duplicate a transition event.

## Composition boundary

The repository currently defines the full `Coordination` trait and feature managers but has no concrete implementation that composes F01 project providers with F02–F07 services. F08 deliberately accepts injected `Arc<dyn Coordination>` and `Arc<dyn SessionAuthenticator>` instead of implementing or duplicating feature policy. F09 owns executable startup and constructing that concrete composition. F10 artifact discovery uses the existing `Coordination::locate_artifacts` contract when its exporter is merged. This boundary permits transport and authentication behavior to be tested against a real shared F02 database while keeping the feature owners' policy authoritative.

## Consequences

Multiple stdio clients share one database and reconnect by opening a new local socket connection for each tool call. SQLite, event cursors, messages, and assignments remain durable across coordinator restarts. The coordinator runner is a library API rather than an executable in F08; the F09 application layer must call it with the fully composed services and place its socket in the dedicated state directory. The internal socket line protocol is private to these adapters and is not a second public API.
