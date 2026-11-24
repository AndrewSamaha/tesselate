//! Coordinator-issued credentials, durable sessions, assignment leases, and messages (F05).

use super::tasks::{
    AssignmentLeaseStatus, AssignmentRecord, AssignmentStatus, LeaseFence, TaskRecord,
};
use crate::{
    core::*,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
        StatePrecondition,
    },
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const BOOTSTRAP_RECORD_ID: &str = "bootstrap";
const DEFAULT_SESSION_TTL_HOURS: i64 = 12;
const DEFAULT_LEASE_TTL_MINUTES: i64 = 2;
const PAGE_SIZE: u32 = 500;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CredentialStatus {
    BootstrapPending,
    Active,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CredentialRecord {
    session_id: SessionId,
    actor_id: Option<ActorId>,
    role: Option<Role>,
    verifier_hash: String,
    status: CredentialStatus,
    expires_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssignmentLeaseRecord {
    pub assignment_id: String,
    pub project_id: ProjectId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub owner_id: ActorId,
    pub session_id: SessionId,
    pub generation: u64,
    pub expires_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CredentialIssuance {
    role: Role,
    actor_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BootstrapIssuance {
    verifier_hash: String,
    actor_name: String,
}

/// F05 authentication, credential issuance, and lease operations. Bearer tokens are never
/// included in mutation requests, event payloads, or stored session records.
pub struct SessionManager {
    db: Database,
    session_ttl: Duration,
    lease_ttl: Duration,
}

impl SessionManager {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            session_ttl: Duration::hours(DEFAULT_SESSION_TTL_HOURS),
            lease_ttl: Duration::minutes(DEFAULT_LEASE_TTL_MINUTES),
        }
    }

    pub fn with_ttls(db: Database, session_ttl: Duration, lease_ttl: Duration) -> CoreResult<Self> {
        if session_ttl <= Duration::zero() || lease_ttl <= Duration::zero() {
            return Err(CoreError::Invalid(
                "session and lease lifetimes must be positive".into(),
            ));
        }
        Ok(Self {
            db,
            session_ttl,
            lease_ttl,
        })
    }

    /// Creates the single-use initial supervisor bootstrap verifier and returns the material once.
    /// F08/F09 own how this value is presented or written to an owner-only file.
    pub fn prepare_bootstrap(&self) -> CoreResult<String> {
        if self
            .db
            .load::<CredentialRecord>(EntityKind::Credential, BOOTSTRAP_RECORD_ID)
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CoreError::Invalid(
                "bootstrap has already been initialized".into(),
            ));
        }
        let secret = random_token();
        let record = CredentialRecord {
            session_id: SessionId(Uuid::nil()),
            actor_id: None,
            role: None,
            verifier_hash: digest(&secret),
            status: CredentialStatus::BootstrapPending,
            expires_at: None,
        };
        let ctx =
            SessionContext::validated(SessionId(Uuid::nil()), ActorId::new(), Role::Supervisor);
        let event = NewEvent::from_context(
            &ctx,
            "bootstrap",
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None,
            },
            "credential.bootstrap_prepared",
            json!({}),
        );
        self.db
            .commit_mutation(MutationInput {
                context: &ctx,
                operation: "credentials.bootstrap.prepare",
                idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
                request: &json!({"bootstrap":true}),
                preconditions: vec![],
                result: &json!({"prepared":true}),
                changes: vec![StateChange::insert(
                    EntityKind::Credential,
                    BOOTSTRAP_RECORD_ID,
                    EntityScope::default(),
                    &record,
                )
                .map_err(storage_error)?],
                event,
            })
            .map_err(storage_error)?;
        Ok(secret)
    }

    /// Local CLI recovery only. This deliberately bypasses expiry, not revocation, and must
    /// never be dispatched through an authenticated socket or MCP operation.
    pub fn recover_local_supervisor(
        &self,
        old_secret: &str,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<SessionContext> {
        let verifier_hash = digest(old_secret);
        let (mut record, revision) = self
            .db
            .load::<CredentialRecord>(EntityKind::Credential, &verifier_hash)
            .map_err(storage_error)?
            .ok_or(CoreError::Unauthenticated)?;
        if !constant_time_eq(&record.verifier_hash, &verifier_hash)
            || record.status != CredentialStatus::Active
            || record.role != Some(Role::Supervisor)
        {
            return Err(CoreError::Unauthenticated);
        }
        let ctx = SessionContext::validated(
            record.session_id,
            record.actor_id.ok_or(CoreError::Unauthenticated)?,
            Role::Supervisor,
        );
        record.expires_at = Some(Utc::now() + self.session_ttl);
        let event = session_event(
            &ctx,
            "session.supervisor_recovered",
            json!({"session_id":record.session_id}),
        );
        self.commit(
            &ctx,
            "sessions.supervisor.recover_local",
            &idempotency_key,
            &record.session_id,
            &record.expires_at.unwrap(),
            vec![StateChange::update(
                EntityKind::Credential,
                verifier_hash,
                credential_scope(&record),
                revision,
                &record,
            )
            .map_err(storage_error)?],
            vec![],
            event,
        )?;
        Ok(ctx)
    }

    pub fn renew_session(
        &self,
        ctx: &SessionContext,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<Mutation<chrono::DateTime<Utc>>> {
        let (credential_key, mut record, revision) = self.find_session(ctx.session_id())?;
        self.validate_context(ctx, &record)?;
        let request = ctx.session_id();
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, "sessions.renew", &idempotency_key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        record.expires_at = Some(Utc::now() + self.session_ttl);
        // The credential row update makes concurrent renewal/revocation serialize; the stored
        // result is the first committed expiry for this idempotency key.
        let event = NewEvent::from_context(
            ctx,
            ctx.actor_id().0.to_string(),
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None,
            },
            "session.renewed",
            json!({"session_id":ctx.session_id()}),
        );
        self.commit(
            ctx,
            "sessions.renew",
            &idempotency_key,
            &request,
            &record.expires_at.unwrap(),
            vec![StateChange::update(
                EntityKind::Credential,
                credential_key,
                credential_scope(&record),
                revision,
                &record,
            )
            .map_err(storage_error)?],
            vec![],
            event,
        )
    }

    /// Starts or renews the lease for the task's current assignment. The assignment revision is
    /// a same-transaction precondition, closing the renewal-after-reassignment race.
    pub fn renew_assignment_lease(
        &self,
        ctx: &SessionContext,
        task_id: TaskId,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<Mutation<AssignmentLeaseRecord>> {
        let context_guard = self.context_guard(ctx)?;
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, "sessions.lease.renew", &idempotency_key, &task_id)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        if ctx.role() != Role::Coder {
            return Err(CoreError::Forbidden);
        }
        let (task, _) = self
            .db
            .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        if task.task.owner != Some(ctx.actor_id())
            || !matches!(task.task.status, TaskStatus::Assigned | TaskStatus::Coding)
        {
            return Err(CoreError::Forbidden);
        }
        let (assignment, assignment_revision) = self.current_assignment(task_id, ctx.actor_id())?;
        let (run, _) = self
            .db
            .load::<Run>(EntityKind::Run, &task.task.run_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let (prior, lease_revision) = self
            .db
            .load::<AssignmentLeaseRecord>(EntityKind::AssignmentLease, &assignment.id)
            .map_err(storage_error)?
            .map(|(lease, rev)| (Some(lease), Some(rev)))
            .unwrap_or((None, None));
        if prior.as_ref().is_some_and(|lease| {
            lease.expires_at > Utc::now() && lease.session_id != ctx.session_id()
        }) {
            return Err(CoreError::Recoverable(
                "another live session owns this assignment lease".into(),
            ));
        }
        let record = AssignmentLeaseRecord {
            assignment_id: assignment.id.clone(),
            project_id: run.project_id,
            run_id: run.id,
            task_id,
            owner_id: ctx.actor_id(),
            session_id: ctx.session_id(),
            generation: prior.map_or(1, |lease| lease.generation + 1),
            expires_at: Utc::now() + self.lease_ttl,
        };
        let change = match lease_revision {
            Some(revision) => StateChange::update(
                EntityKind::AssignmentLease,
                &record.assignment_id,
                assignment_lease_scope(&task.task, &record),
                revision,
                &record,
            ),
            None => StateChange::insert(
                EntityKind::AssignmentLease,
                &record.assignment_id,
                assignment_lease_scope(&task.task, &record),
                &record,
            ),
        }
        .map_err(storage_error)?;
        let event = NewEvent::from_context(
            ctx,
            ctx.actor_id().0.to_string(),
            EventScope {
                project_id: Some(run.project_id),
                run_id: Some(run.id),
                task_id: Some(task_id),
            },
            "assignment.lease_renewed",
            json!({"assignment_id":record.assignment_id,"generation":record.generation,"expires_at":record.expires_at}),
        );
        self.commit(
            ctx,
            "sessions.lease.renew",
            &idempotency_key,
            &task_id,
            &record,
            vec![change],
            vec![
                StatePrecondition {
                    kind: EntityKind::Assignment,
                    id: assignment.id,
                    expected_revision: assignment_revision,
                },
                context_guard,
            ],
            event,
        )
    }

    pub fn expire_session(
        &self,
        supervisor: &SessionContext,
        session_id: SessionId,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<()> {
        let supervisor_guard = self.require_supervisor(supervisor)?;
        if self
            .db
            .prior_mutation::<_, bool>(supervisor, "sessions.expire", &idempotency_key, &session_id)
            .map_err(storage_error)?
            .is_some()
        {
            return Ok(());
        }
        let (key, mut credential, revision) = self.find_session(session_id)?;
        let changed = credential.status == CredentialStatus::Active;
        if changed {
            credential.status = CredentialStatus::Expired;
        }
        let mut changes = vec![StateChange::update(
            EntityKind::Credential,
            key,
            credential_scope(&credential),
            revision,
            &credential,
        )
        .map_err(storage_error)?];
        for (id, mut lease, lease_revision) in if changed {
            self.leases_for_session(session_id)?
        } else {
            vec![]
        } {
            lease.expires_at = Utc::now();
            lease.generation += 1;
            changes.push(
                StateChange::update(
                    EntityKind::AssignmentLease,
                    id.clone(),
                    lease_scope(&lease),
                    lease_revision,
                    &lease,
                )
                .map_err(storage_error)?,
            );
        }
        let event = session_event(
            supervisor,
            if changed {
                "session.expired"
            } else {
                "session.expire_noop"
            },
            json!({"session_id":session_id}),
        );
        self.commit(
            supervisor,
            "sessions.expire",
            &idempotency_key,
            &session_id,
            &true,
            changes,
            vec![supervisor_guard],
            event,
        )?;
        Ok(())
    }

    fn issue(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        role: Role,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        let supervisor_guard = self.require_supervisor(supervisor)?;
        if actor_name.trim().is_empty() {
            return Err(CoreError::Invalid("actor name is required".into()));
        }
        let request = CredentialIssuance {
            role,
            actor_name: actor_name.trim().into(),
        };
        if let Some(prior) = self
            .db
            .prior_mutation::<_, SessionId>(
                supervisor,
                "credentials.issue",
                &idempotency_key,
                &request,
            )
            .map_err(storage_error)?
        {
            let (key, record, _) = self.find_session(prior.value)?;
            let _ = key;
            return Ok(IssuedCredential {
                session_id: prior.value,
                actor_id: record.actor_id.ok_or(CoreError::Unauthenticated)?,
                secret: None,
                replayed: true,
            });
        }
        let actor = Actor {
            id: ActorId::new(),
            display_name: actor_name.trim().into(),
            role,
        };
        let session_id = SessionId::new();
        let secret = random_token();
        let verifier_hash = digest(&secret);
        let record = CredentialRecord {
            session_id,
            actor_id: Some(actor.id),
            role: Some(role),
            verifier_hash: verifier_hash.clone(),
            status: CredentialStatus::Active,
            expires_at: Some(Utc::now() + self.session_ttl),
        };
        let request = CredentialIssuance {
            role,
            actor_name: actor.display_name.clone(),
        };
        let event = session_event(
            supervisor,
            "credential.issued",
            json!({"actor_id":actor.id,"role":role,"session_id":session_id}),
        );
        let committed = self.commit(
            supervisor,
            "credentials.issue",
            &idempotency_key,
            &request,
            &session_id,
            vec![
                StateChange::insert(
                    EntityKind::Actor,
                    actor.id.0.to_string(),
                    EntityScope::default(),
                    &actor,
                )
                .map_err(storage_error)?,
                StateChange::insert(
                    EntityKind::Credential,
                    verifier_hash,
                    credential_scope(&record),
                    &record,
                )
                .map_err(storage_error)?,
            ],
            vec![supervisor_guard],
            event,
        )?;
        if committed.replayed {
            let (_, record, _) = self.find_session(committed.value)?;
            return Ok(IssuedCredential {
                session_id: committed.value,
                actor_id: record.actor_id.ok_or(CoreError::Unauthenticated)?,
                secret: None,
                replayed: true,
            });
        }
        Ok(IssuedCredential {
            session_id,
            actor_id: actor.id,
            secret: Some(secret),
            replayed: false,
        })
    }

    fn validate_context(&self, ctx: &SessionContext, record: &CredentialRecord) -> CoreResult<()> {
        if record.status != CredentialStatus::Active
            || record
                .expires_at
                .is_none_or(|expires| expires <= Utc::now())
            || record.actor_id != Some(ctx.actor_id())
            || record.role != Some(ctx.role())
        {
            return Err(CoreError::Unauthenticated);
        }
        Ok(())
    }

    fn context_guard(&self, ctx: &SessionContext) -> CoreResult<StatePrecondition> {
        let (key, record, revision) = self.find_session(ctx.session_id())?;
        self.validate_context(ctx, &record)?;
        Ok(StatePrecondition {
            kind: EntityKind::Credential,
            id: key,
            expected_revision: revision,
        })
    }

    fn require_supervisor(&self, ctx: &SessionContext) -> CoreResult<StatePrecondition> {
        let guard = self.context_guard(ctx)?;
        if ctx.role() != Role::Supervisor {
            return Err(CoreError::Forbidden);
        }
        Ok(guard)
    }

    fn find_session(&self, session_id: SessionId) -> CoreResult<(String, CredentialRecord, i64)> {
        self.all::<CredentialRecord>(EntityKind::Credential, EntityScope::default())?
            .into_iter()
            .find(|record| record.session_id == session_id && record.actor_id.is_some())
            .map(|record| {
                let key = record.verifier_hash.clone();
                let (current, revision) = self.load_credential(&key)?;
                Ok((key, current, revision))
            })
            .transpose()?
            .ok_or(CoreError::NotFound)
    }

    fn load_credential(&self, key: &str) -> CoreResult<(CredentialRecord, i64)> {
        self.db
            .load(EntityKind::Credential, key)
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn current_assignment(
        &self,
        task_id: TaskId,
        owner: ActorId,
    ) -> CoreResult<(AssignmentRecord, i64)> {
        let records = self.all::<AssignmentRecord>(
            EntityKind::Assignment,
            EntityScope {
                task_id: Some(task_id.0.to_string()),
                owner_id: Some(owner.0.to_string()),
                ..Default::default()
            },
        )?;
        let assignment = records
            .into_iter()
            .filter(|record| record.status == AssignmentStatus::Active)
            .max_by_key(|record| record.assigned_at)
            .ok_or(CoreError::NotFound)?;
        self.db
            .load(EntityKind::Assignment, &assignment.id)
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn leases_for_session(
        &self,
        session_id: SessionId,
    ) -> CoreResult<Vec<(String, AssignmentLeaseRecord, i64)>> {
        let leases =
            self.all::<AssignmentLeaseRecord>(EntityKind::AssignmentLease, EntityScope::default())?;
        leases
            .into_iter()
            .filter(|lease| lease.session_id == session_id)
            .map(|lease| {
                let loaded = self
                    .db
                    .load::<AssignmentLeaseRecord>(
                        EntityKind::AssignmentLease,
                        &lease.assignment_id,
                    )
                    .map_err(storage_error)?
                    .ok_or(CoreError::NotFound)?;
                Ok((lease.assignment_id, loaded.0, loaded.1))
            })
            .collect()
    }

    fn all<T: for<'de> Deserialize<'de>>(
        &self,
        kind: EntityKind,
        scope: EntityScope,
    ) -> CoreResult<Vec<T>> {
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = self
                .db
                .list_entities(
                    kind,
                    &scope,
                    PageRequest {
                        limit: PAGE_SIZE,
                        after,
                    },
                )
                .map_err(storage_error)?;
            let len = page.items.len();
            all.extend(page.items);
            if len < PAGE_SIZE as usize {
                break;
            }
            after = page.next_cursor.map(EventCursor);
        }
        Ok(all)
    }

    fn commit<Q: Serialize, R: Serialize + for<'de> Deserialize<'de>>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        result: &R,
        changes: Vec<StateChange>,
        preconditions: Vec<StatePrecondition>,
        event: NewEvent,
    ) -> CoreResult<Mutation<R>> {
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation,
                idempotency_key: key,
                request,
                preconditions,
                result,
                changes,
                event,
            })
            .map_err(storage_error)
    }

    fn leases_for_credential_revocation(
        &self,
        session_id: SessionId,
    ) -> CoreResult<Vec<(String, AssignmentLeaseRecord, i64)>> {
        self.leases_for_session(session_id)
    }

    pub fn recoverable_leases(
        &self,
        supervisor: &SessionContext,
        project_id: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<AssignmentLeaseRecord>> {
        self.require_supervisor(supervisor)?;
        let leases = self
            .db
            .list_entities(
                EntityKind::AssignmentLease,
                &EntityScope {
                    project_id: Some(project_id.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(storage_error)?;
        let mut items = Vec::new();
        for lease in leases
            .items
            .into_iter()
            .filter(|lease: &AssignmentLeaseRecord| lease.expires_at <= Utc::now())
        {
            let assignment = self
                .db
                .load::<AssignmentRecord>(EntityKind::Assignment, &lease.assignment_id)
                .map_err(storage_error)?;
            let task = self
                .db
                .load::<TaskRecord>(EntityKind::Task, &lease.task_id.0.to_string())
                .map_err(storage_error)?;
            if assignment
                .is_some_and(|(assignment, _)| assignment.status == AssignmentStatus::Active)
                && task.is_some_and(|(task, _)| {
                    task.task.owner == Some(lease.owner_id)
                        && matches!(task.task.status, TaskStatus::Assigned | TaskStatus::Coding)
                })
            {
                items.push(lease);
            }
        }
        Ok(Page {
            items,
            next_cursor: leases.next_cursor,
        })
    }

    pub fn send_message(
        &self,
        ctx: &SessionContext,
        request: SendMessage,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Message>> {
        let context_guard = self.context_guard(ctx)?;
        if !matches!(ctx.role(), Role::Supervisor | Role::Coder | Role::Reviewer) {
            return Err(CoreError::Forbidden);
        }
        if request.body.trim().is_empty() {
            return Err(CoreError::Invalid("message body is required".into()));
        }
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, "messages.send", &key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        self.db
            .load::<Project>(EntityKind::Project, &request.project_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let mut message_run_id = request.run_id;
        if let Some(task_id) = request.task_id {
            let (task, _) = self
                .db
                .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            let (run, _) = self
                .db
                .load::<Run>(EntityKind::Run, &task.task.run_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            if run.project_id != request.project_id || request.run_id.is_some_and(|id| id != run.id)
            {
                return Err(CoreError::Invalid(
                    "message scope does not match task".into(),
                ));
            }
            if ctx.role() == Role::Coder && task.task.owner != Some(ctx.actor_id()) {
                return Err(CoreError::Forbidden);
            }
            if ctx.role() == Role::Reviewer && task.task.status != TaskStatus::InReview {
                return Err(CoreError::Forbidden);
            }
            message_run_id = Some(run.id);
        } else if let Some(run_id) = request.run_id {
            let (run, _) = self
                .db
                .load::<Run>(EntityKind::Run, &run_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            if run.project_id != request.project_id {
                return Err(CoreError::Invalid(
                    "message scope does not match run".into(),
                ));
            }
        }
        let message = Message {
            id: MessageId::new(),
            project_id: request.project_id,
            run_id: message_run_id,
            task_id: request.task_id,
            sender: ctx.actor_id(),
            body: request.body.clone(),
            event_id: EventId::new(),
        };
        let event = NewEvent::from_context(
            ctx,
            ctx.actor_id().0.to_string(),
            EventScope {
                project_id: Some(message.project_id),
                run_id: message.run_id,
                task_id: message.task_id,
            },
            "message.sent",
            json!({"message_id":message.id,"sender":message.sender}),
        );
        let mut message = message;
        message.event_id = event.event_id;
        let scope = EntityScope {
            project_id: Some(message.project_id.0.to_string()),
            run_id: message.run_id.map(|id| id.0.to_string()),
            task_id: message.task_id.map(|id| id.0.to_string()),
            owner_id: Some(message.sender.0.to_string()),
        };
        self.commit(
            ctx,
            "messages.send",
            &key,
            &request,
            &message,
            vec![StateChange::insert(
                EntityKind::Message,
                message.id.0.to_string(),
                scope,
                &message,
            )
            .map_err(storage_error)?],
            vec![context_guard],
            event,
        )
    }

    pub fn read_messages(
        &self,
        ctx: &SessionContext,
        request: ReadMessages,
    ) -> CoreResult<Page<Message>> {
        self.context_guard(ctx)?;
        if !matches!(ctx.role(), Role::Supervisor | Role::Coder | Role::Reviewer) {
            return Err(CoreError::Forbidden);
        }
        self.db
            .load::<Project>(EntityKind::Project, &request.project_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        if let Some(task_id) = request.task_id {
            let (task, _) = self
                .db
                .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            let (run, _) = self
                .db
                .load::<Run>(EntityKind::Run, &task.task.run_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            if run.project_id != request.project_id {
                return Err(CoreError::Invalid(
                    "message scope does not match task".into(),
                ));
            }
        }
        self.db
            .messages_after(
                request.project_id,
                request.task_id,
                request.page.after.as_ref(),
                request.page.limit,
            )
            .map_err(storage_error)
    }
}

impl CredentialIssuer for SessionManager {
    fn bootstrap_supervisor(
        &self,
        bootstrap_secret: &str,
        actor_name: &str,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        if actor_name.trim().is_empty() {
            return Err(CoreError::Invalid("actor name is required".into()));
        }
        let request = BootstrapIssuance {
            verifier_hash: digest(bootstrap_secret),
            actor_name: actor_name.trim().into(),
        };
        let ctx = SessionContext::validated(
            SessionId(Uuid::nil()),
            ActorId(Uuid::nil()),
            Role::Supervisor,
        );
        if let Some(prior) = self
            .db
            .prior_mutation::<_, SessionId>(
                &ctx,
                "credentials.bootstrap.consume",
                &idempotency_key,
                &request,
            )
            .map_err(storage_error)?
        {
            let (_, record, _) = self.find_session(prior.value)?;
            return Ok(IssuedCredential {
                session_id: prior.value,
                actor_id: record.actor_id.ok_or(CoreError::Unauthenticated)?,
                secret: None,
                replayed: true,
            });
        }
        let (mut bootstrap, revision) = self.load_credential(BOOTSTRAP_RECORD_ID)?;
        if bootstrap.status != CredentialStatus::BootstrapPending
            || !constant_time_eq(&bootstrap.verifier_hash, &digest(bootstrap_secret))
        {
            return Err(CoreError::Unauthenticated);
        }
        let actor = Actor {
            id: ActorId::new(),
            display_name: actor_name.trim().into(),
            role: Role::Supervisor,
        };
        let session_id = SessionId::new();
        let secret = random_token();
        let verifier_hash = digest(&secret);
        bootstrap.status = CredentialStatus::Revoked;
        bootstrap.verifier_hash.clear();
        let credential = CredentialRecord {
            session_id,
            actor_id: Some(actor.id),
            role: Some(Role::Supervisor),
            verifier_hash: verifier_hash.clone(),
            status: CredentialStatus::Active,
            expires_at: Some(Utc::now() + self.session_ttl),
        };
        let event = NewEvent::from_context(
            &ctx,
            "local-bootstrap",
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None,
            },
            "credential.supervisor_bootstrapped",
            json!({"actor_id":actor.id,"session_id":session_id}),
        );
        let committed = self.commit(
            &ctx,
            "credentials.bootstrap.consume",
            &idempotency_key,
            &request,
            &session_id,
            vec![
                StateChange::update(
                    EntityKind::Credential,
                    BOOTSTRAP_RECORD_ID,
                    EntityScope::default(),
                    revision,
                    &bootstrap,
                )
                .map_err(storage_error)?,
                StateChange::insert(
                    EntityKind::Actor,
                    actor.id.0.to_string(),
                    EntityScope::default(),
                    &actor,
                )
                .map_err(storage_error)?,
                StateChange::insert(
                    EntityKind::Credential,
                    verifier_hash,
                    credential_scope(&credential),
                    &credential,
                )
                .map_err(storage_error)?,
            ],
            vec![],
            event,
        )?;
        if committed.replayed {
            let (_, record, _) = self.find_session(committed.value)?;
            return Ok(IssuedCredential {
                session_id: committed.value,
                actor_id: record.actor_id.ok_or(CoreError::Unauthenticated)?,
                secret: None,
                replayed: true,
            });
        }
        Ok(IssuedCredential {
            session_id,
            actor_id: actor.id,
            secret: Some(secret),
            replayed: false,
        })
    }

    fn issue_agent(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        role: AgentRole,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        self.issue(
            supervisor,
            actor_name,
            match role {
                AgentRole::Coder => Role::Coder,
                AgentRole::Reviewer => Role::Reviewer,
            },
            idempotency_key,
        )
    }

    fn issue_cli(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        self.issue(supervisor, actor_name, Role::Cli, idempotency_key)
    }

    fn revoke(
        &self,
        supervisor: &SessionContext,
        session: SessionId,
        idempotency_key: IdempotencyKey,
    ) -> CoreResult<()> {
        let supervisor_guard = self.require_supervisor(supervisor)?;
        if self
            .db
            .prior_mutation::<_, bool>(supervisor, "credentials.revoke", &idempotency_key, &session)
            .map_err(storage_error)?
            .is_some()
        {
            return Ok(());
        }
        let (key, mut record, revision) = self.find_session(session)?;
        let changed = record.status != CredentialStatus::Revoked;
        if changed {
            record.status = CredentialStatus::Revoked;
        }
        let mut changes = vec![StateChange::update(
            EntityKind::Credential,
            key,
            credential_scope(&record),
            revision,
            &record,
        )
        .map_err(storage_error)?];
        for (id, mut lease, lease_revision) in if changed {
            self.leases_for_credential_revocation(session)?
        } else {
            vec![]
        } {
            lease.expires_at = Utc::now();
            lease.generation += 1;
            changes.push(
                StateChange::update(
                    EntityKind::AssignmentLease,
                    id.clone(),
                    lease_scope(&lease),
                    lease_revision,
                    &lease,
                )
                .map_err(storage_error)?,
            );
        }
        let event = session_event(
            supervisor,
            if changed {
                "credential.revoked"
            } else {
                "credential.revoke_noop"
            },
            json!({"session_id":session}),
        );
        self.commit(
            supervisor,
            "credentials.revoke",
            &idempotency_key,
            &session,
            &true,
            changes,
            vec![supervisor_guard],
            event,
        )?;
        Ok(())
    }
}

impl SessionAuthenticator for SessionManager {
    fn authenticate(&self, bearer_credential: &str) -> CoreResult<SessionContext> {
        let verifier_hash = digest(bearer_credential);
        let (record, _) = self
            .db
            .load::<CredentialRecord>(EntityKind::Credential, &verifier_hash)
            .map_err(storage_error)?
            .ok_or(CoreError::Unauthenticated)?;
        if !constant_time_eq(&record.verifier_hash, &verifier_hash)
            || record.status != CredentialStatus::Active
            || record.expires_at.is_none_or(|expiry| expiry <= Utc::now())
        {
            return Err(CoreError::Unauthenticated);
        }
        Ok(SessionContext::validated(
            record.session_id,
            record.actor_id.ok_or(CoreError::Unauthenticated)?,
            record.role.ok_or(CoreError::Unauthenticated)?,
        ))
    }
}

impl AssignmentLeaseStatus for SessionManager {
    fn expired_lease_fence(&self, task_id: TaskId, owner: ActorId) -> CoreResult<LeaseFence> {
        let (task, _) = self
            .db
            .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        if task.task.owner != Some(owner)
            || !matches!(task.task.status, TaskStatus::Assigned | TaskStatus::Coding)
        {
            return Err(CoreError::Recoverable(
                "assignment is no longer active".into(),
            ));
        }
        let (assignment, _) = self.current_assignment(task_id, owner)?;
        let (lease, revision) = self
            .db
            .load::<AssignmentLeaseRecord>(EntityKind::AssignmentLease, &assignment.id)
            .map_err(storage_error)?
            .ok_or_else(|| CoreError::Recoverable("assignment lease has not been opened".into()))?;
        if lease.expires_at > Utc::now() {
            return Err(CoreError::Recoverable(
                "assignment lease is still active".into(),
            ));
        }
        Ok(LeaseFence {
            task_id,
            owner_id: owner,
            condition: StatePrecondition {
                kind: EntityKind::AssignmentLease,
                id: assignment.id,
                expected_revision: revision,
            },
        })
    }
}

fn credential_scope(record: &CredentialRecord) -> EntityScope {
    EntityScope {
        owner_id: record.actor_id.map(|id| id.0.to_string()),
        ..Default::default()
    }
}
fn assignment_lease_scope(_task: &Task, lease: &AssignmentLeaseRecord) -> EntityScope {
    lease_scope(lease)
}
fn lease_scope(lease: &AssignmentLeaseRecord) -> EntityScope {
    EntityScope {
        project_id: Some(lease.project_id.0.to_string()),
        run_id: Some(lease.run_id.0.to_string()),
        task_id: Some(lease.task_id.0.to_string()),
        owner_id: Some(lease.owner_id.0.to_string()),
    }
}
fn session_event(ctx: &SessionContext, event_type: &str, payload: serde_json::Value) -> NewEvent {
    NewEvent::from_context(
        ctx,
        ctx.actor_id().0.to_string(),
        EventScope {
            project_id: None,
            run_id: None,
            task_id: None,
        },
        event_type,
        payload,
    )
}
fn random_token() -> String {
    format!("{}.{}.{}", Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4())
}
fn digest(secret: &str) -> String {
    format!("{:x}", Sha256::digest(secret.as_bytes()))
}
fn constant_time_eq(left: &str, right: &str) -> bool {
    let mut diff = left.len() ^ right.len();
    for (a, b) in left.bytes().zip(right.bytes()) {
        diff |= (a ^ b) as usize;
    }
    diff == 0
}
fn storage_error(error: PersistenceError) -> CoreError {
    match error {
        PersistenceError::IdempotencyConflict => CoreError::IdempotencyConflict,
        PersistenceError::StateConflict(id) => {
            CoreError::Recoverable(format!("state changed concurrently: {id}"))
        }
        PersistenceError::Invalid(message) => CoreError::Invalid(message),
        other => CoreError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod local_recovery_tests {
    use super::*;
    use std::{fs, path::PathBuf};

    struct TempDb(PathBuf);

    impl TempDb {
        fn new() -> Self {
            let directory = std::env::temp_dir().join(format!("tesselate-recovery-{}", Uuid::new_v4()));
            fs::create_dir_all(&directory).unwrap();
            Self(directory.join("state.sqlite3"))
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0.parent().unwrap());
        }
    }

    #[test]
    fn only_exact_active_supervisor_secret_can_recover_a_lapsed_session() {
        let temp = TempDb::new();
        let db = Database::open(&temp.0).unwrap();
        let manager = SessionManager::new(db.clone());
        let bootstrap = manager.prepare_bootstrap().unwrap();
        let supervisor = manager
            .bootstrap_supervisor(&bootstrap, "owner", IdempotencyKey("bootstrap".into()))
            .unwrap();
        let secret = supervisor.secret.as_deref().unwrap();
        let context = manager.authenticate(secret).unwrap();
        let coder = manager
            .issue_agent(&context, "coder", AgentRole::Coder, IdempotencyKey("coder".into()))
            .unwrap();
        assert!(matches!(
            manager.recover_local_supervisor(
                coder.secret.as_deref().unwrap(),
                IdempotencyKey("coder-recovery".into())
            ),
            Err(CoreError::Unauthenticated)
        ));
        let verifier_hash = digest(secret);
        let (mut record, revision) = manager.load_credential(&verifier_hash).unwrap();
        record.expires_at = Some(Utc::now() - Duration::days(1));
        db.commit_mutation(MutationInput {
            context: &context,
            operation: "test.supervisor.expire",
            idempotency_key: &IdempotencyKey("test-expiry".into()),
            request: &context.session_id(),
            preconditions: vec![],
            result: &true,
            changes: vec![StateChange::update(
                EntityKind::Credential,
                &verifier_hash,
                credential_scope(&record),
                revision,
                &record,
            )
            .unwrap()],
            event: session_event(&context, "test.session_lapsed", json!({})),
        })
        .unwrap();
        let reopened = SessionManager::new(Database::open(&temp.0).unwrap());
        assert!(matches!(
            reopened.authenticate(secret),
            Err(CoreError::Unauthenticated)
        ));
        assert!(matches!(
            reopened.recover_local_supervisor(&format!("{secret}x"), IdempotencyKey("wrong".into())),
            Err(CoreError::Unauthenticated)
        ));
        let recovered = reopened
            .recover_local_supervisor(secret, IdempotencyKey("recover".into()))
            .unwrap();
        assert_eq!(recovered.session_id(), context.session_id());
        assert_eq!(reopened.authenticate(secret).unwrap().role(), Role::Supervisor);
        let count = db.event_count().unwrap();
        assert_eq!(
            reopened
                .recover_local_supervisor(secret, IdempotencyKey("recover".into()))
                .unwrap()
                .session_id(),
            recovered.session_id()
        );
        assert_eq!(db.event_count().unwrap(), count);
        reopened
            .revoke(
                &recovered,
                recovered.session_id(),
                IdempotencyKey("revoke-supervisor".into()),
            )
            .unwrap();
        assert!(matches!(
            reopened.recover_local_supervisor(secret, IdempotencyKey("recover-again".into())),
            Err(CoreError::Unauthenticated)
        ));
        let events = db
            .events_after(
                EventScope {
                    project_id: None,
                    run_id: None,
                    task_id: None,
                },
                None,
                100,
            )
            .unwrap();
        assert!(!serde_json::to_string(&events).unwrap().contains(secret));
    }
}
