# ADR-0001: Local credential bootstrap

Status: Accepted for M001

The coordinator creates one-time bootstrap material on first initialization, stores only its cryptographic verifier with owner-only permissions, and presents the secret once to the local operator. The bootstrap operation accepts no role argument and can issue only the initial supervisor credential; it invalidates the bootstrap verifier immediately. A supervisor-authorized issuance operation creates coder, reviewer, or CLI credentials (CLI is limited to human decision operations). Store issued opaque credentials in an owner-only credential file (0600), never in project config, logs, events, or artifacts. The coordinator stores only a verifier and binds each credential to actor and role; revoke marks it unusable immediately. Missing or lost credentials are recovered by an explicit local owner-only CLI reset that invalidates all sessions and rotates bootstrap material.

This keeps authority coordinator-issued, supports non-interactive agents after provisioning, and gives local recovery a clear boundary. F08/F09 implement file/socket protections; F05 implements issuance, validation, and revocation.
