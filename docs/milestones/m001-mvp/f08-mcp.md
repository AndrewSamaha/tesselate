# F08: Concurrent MCP service

**Owner:** `mcp` agent. **Depends on:** F00; integrates F03–F07 as they land. **Output:** the agent-facing service and tools.

## Scope

- Run one headless Tokio coordinator process with a permission-restricted Unix socket and state directory. Provide a small stdio MCP adapter for each harness session.
- Implement the F00 versioned tool schemas with `rmcp` for discovery, coordination, communication, workspace, review, human requests, and bounded history.
- Authenticate each connection and pass its session context to the core. Return core errors and event IDs consistently. Mutating tools require idempotency keys.
- Support multiple concurrent sessions and reconnection. Polling for assignments and messages is sufficient for the MVP.
- Keep tools as adapters over core operations, with no duplicate authorization or transition rules.

## Acceptance

- Two or more stdio clients can connect to the same coordinator and observe shared state without launching separate databases.
- An unauthenticated or wrong-role client cannot mutate protected state.
- A client can reconnect after service restart, read missed messages/events, and continue with its retained assignment.
- The tool namespace and JSON shapes match F00's contract tests.

**Boundary:** Tesselate does not launch or host models. F08 exposes the coordinator runner over injected `Coordination` and `SessionAuthenticator` services; F09 owns concrete application composition and process startup. A demonstration harness may launch MCP clients externally.
