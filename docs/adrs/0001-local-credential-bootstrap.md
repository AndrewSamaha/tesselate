# ADR-0001: Local credential bootstrap

Status: Accepted for M001

The coordinator creates one-time bootstrap material on first initialization, stores only its cryptographic verifier with owner-only permissions, and presents the secret once to the local operator. The first CLI bootstrap exchanges it for a supervisor credential; the coordinator invalidates the bootstrap verifier immediately. An authenticated supervisor then issues least-privilege coder and reviewer credentials through the core session API. CLI uses the same authenticated session contract and may have a dedicated local CLI role for human decisions. Store issued opaque credentials in an owner-only credential file (0600), never in project config, logs, events, or artifacts. The coordinator stores only a verifier and binds each credential to actor and role; revoke marks it unusable immediately. Missing or lost credentials are recovered by an explicit local owner-only CLI reset that invalidates all sessions and rotates bootstrap material.

This keeps authority coordinator-issued, supports non-interactive agents after provisioning, and gives local recovery a clear boundary. F08/F09 implement file/socket protections; F05 implements issuance, validation, and revocation.
