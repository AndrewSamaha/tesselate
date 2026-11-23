//! Durable, policy-governed human decisions (F07).

use super::{
    reviews::ReviewHumanGate,
    tasks::{HumanTransitionGate, TransitionContext, TransitionIdentity},
};
use crate::{
    core::*,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
    },
};
use chrono::Utc;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

const OP_REQUEST: &str = "human.request";
const OP_RESOLVE: &str = "human.resolve";
const OP_EXPIRE: &str = "human.expire";

/// F01 supplies the validated human-gate policy for a registered project.
pub trait ProjectHumanGatePolicy: Send + Sync {
    fn human_gate_policy(&self, project_id: ProjectId) -> CoreResult<Option<HumanGatePolicy>>;
}

pub struct HumanCoordinator {
    db: Database,
    policy: Arc<dyn ProjectHumanGatePolicy>,
}

impl HumanCoordinator {
    pub fn new(db: Database, policy: Arc<dyn ProjectHumanGatePolicy>) -> Self {
        Self { db, policy }
    }

    pub fn request_human(
        &self,
        ctx: &SessionContext,
        request: HumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>> {
        if let Some(prior) = self.prior(ctx, OP_REQUEST, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        validate_request(&request)?;
        let run_id = self.validate_scope(request.project_id, request.run_id, request.task_id)?;
        let decision = HumanDecision {
            id: HumanRequestId::new(),
            project_id: request.project_id,
            run_id,
            task_id: request.task_id,
            gate_key: None,
            question: request.question.clone(),
            context: request.context.clone(),
            choices: request.choices.clone(),
            requested_by: ctx.actor_id(),
            created_at: Utc::now(),
            expires_at: request.expires_at,
            on_expiry: request.on_expiry,
            status: HumanStatus::Pending,
            resolution: None,
        };
        self.commit_decision(
            ctx,
            OP_REQUEST,
            &key,
            &request,
            &decision,
            "human.requested",
        )
    }

    pub fn list_human_requests(
        &self,
        ctx: &SessionContext,
        request: ListHumanRequests,
    ) -> CoreResult<Page<HumanDecision>> {
        require_reader(ctx)?;
        // Snapshot all rows before mutating expirations; updates move entities to the end of the
        // sequence, so expiring while walking the cursor could revisit the same rows forever.
        let all = self.all_for_project(request.project_id)?;
        for decision in all {
            self.expire_if_due(ctx, decision.id)?;
        }
        self.db
            .list_entities(
                EntityKind::HumanRequest,
                &EntityScope {
                    project_id: Some(request.project_id.0.to_string()),
                    ..Default::default()
                },
                request.page,
            )
            .map_err(storage_error)
    }

    pub fn resolve_human(
        &self,
        ctx: &SessionContext,
        request: ResolveHumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>> {
        if let Some(prior) = self.prior(ctx, OP_RESOLVE, &key, &request)? {
            return Ok(prior);
        }
        require_resolver(ctx)?;
        let (mut decision, revision) = self.load(request.request_id)?;
        if !decision
            .choices
            .iter()
            .any(|choice| choice == &request.choice)
        {
            return Err(CoreError::Invalid(
                "choice is not one of this request's choices".into(),
            ));
        }
        if request.reason.trim().is_empty() {
            return Err(CoreError::Invalid("resolution reason is required".into()));
        }
        match decision.status {
            HumanStatus::Pending => {
                if self.expire_if_due(ctx, request.request_id)? {
                    return Err(CoreError::Recoverable(
                        "human request expired before resolution".into(),
                    ));
                }
                decision.status = HumanStatus::Resolved;
                decision.resolution = Some(HumanResolution {
                    choice: request.choice.clone(),
                    actor_id: ctx.actor_id(),
                    reason: request.reason.clone(),
                    resolved_at: Utc::now(),
                });
                let change = StateChange::update(
                    EntityKind::HumanRequest,
                    decision.id.0.to_string(),
                    scope(&decision),
                    revision,
                    &decision,
                )
                .map_err(storage_error)?;
                self.commit_decision_with_change(
                    ctx,
                    OP_RESOLVE,
                    &key,
                    &request,
                    &decision,
                    change,
                    "human.resolved",
                )
            }
            HumanStatus::Resolved => {
                let prior = decision.resolution.as_ref().ok_or_else(|| {
                    CoreError::Internal("resolved request has no resolution".into())
                })?;
                if prior.actor_id != ctx.actor_id()
                    || prior.choice != request.choice
                    || prior.reason != request.reason
                {
                    return Err(CoreError::Invalid(
                        "request already has a different resolution".into(),
                    ));
                }
                let change = StateChange::update(
                    EntityKind::HumanRequest,
                    decision.id.0.to_string(),
                    scope(&decision),
                    revision,
                    &decision,
                )
                .map_err(storage_error)?;
                self.commit_decision_with_change(
                    ctx,
                    OP_RESOLVE,
                    &key,
                    &request,
                    &decision,
                    change,
                    "human.resolution_reaffirmed",
                )
            }
            HumanStatus::Expired | HumanStatus::Escalated => Err(CoreError::Recoverable(
                "human request is no longer resolvable".into(),
            )),
        }
    }

    fn validate_scope(
        &self,
        project_id: ProjectId,
        run_id: Option<RunId>,
        task_id: Option<TaskId>,
    ) -> CoreResult<Option<RunId>> {
        self.db
            .load::<Project>(EntityKind::Project, &project_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let resolved_run = if let Some(task_id) = task_id {
            let (record, _) = self
                .db
                .load::<super::tasks::TaskRecord>(EntityKind::Task, &task_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            let (run, _) = self
                .db
                .load::<Run>(EntityKind::Run, &record.task.run_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            if run.project_id != project_id || run_id.is_some_and(|id| id != run.id) {
                return Err(CoreError::Invalid(
                    "human request scope does not match its task".into(),
                ));
            }
            Some(run.id)
        } else {
            None
        };
        let run_id = resolved_run.or(run_id);
        if let Some(run_id) = run_id {
            let (run, _) = self
                .db
                .load::<Run>(EntityKind::Run, &run_id.0.to_string())
                .map_err(storage_error)?
                .ok_or(CoreError::NotFound)?;
            if run.project_id != project_id {
                return Err(CoreError::Invalid(
                    "human request run belongs to another project".into(),
                ));
            }
        }
        Ok(run_id)
    }

    fn require_gate(
        &self,
        ctx: &SessionContext,
        project_id: ProjectId,
        run_id: RunId,
        task_id: Option<TaskId>,
        gate_key: String,
        question: String,
        context: String,
    ) -> CoreResult<()> {
        let evidence_generation = if gate_key.starts_with("run_completion:") {
            Some(self.completion_evidence_generation(run_id)?)
        } else {
            None
        };
        let gate_key = evidence_generation.map_or(gate_key.clone(), |generation| {
            format!("{gate_key}:evidence:{generation}")
        });
        let id = gate_id(&gate_key);
        let mut decision = if let Some((decision, _)) = self
            .db
            .load::<HumanDecision>(EntityKind::HumanRequest, &id.0.to_string())
            .map_err(storage_error)?
        {
            decision
        } else {
            let request = GateRequest {
                gate_key: &gate_key,
                question: &question,
                context: &context,
            };
            let created = HumanDecision {
                id,
                project_id,
                run_id: Some(run_id),
                task_id,
                gate_key: Some(gate_key.clone()),
                question: question.clone(),
                context: context.clone(),
                choices: vec!["approve".into(), "reject".into()],
                requested_by: ctx.actor_id(),
                created_at: Utc::now(),
                expires_at: None,
                on_expiry: None,
                status: HumanStatus::Pending,
                resolution: None,
            };
            let event_type = if gate_key.starts_with("review_deferral:") {
                "human.review_deferral_requested"
            } else {
                "human.transition_requested"
            };
            let key = IdempotencyKey(format!("gate:{}", full_hash(&gate_key)));
            match self.commit_decision(ctx, OP_REQUEST, &key, &request, &created, event_type) {
                Ok(_) | Err(CoreError::Recoverable(_)) => self.load(id)?.0,
                Err(error) => return Err(error),
            }
        };
        self.expire_if_due(ctx, decision.id)?;
        decision = self.load(id)?.0;
        match decision.status {
            HumanStatus::Resolved
                if decision
                    .resolution
                    .as_ref()
                    .is_some_and(|resolution| resolution.choice == "approve") =>
            {
                Ok(())
            }
            HumanStatus::Resolved => Err(CoreError::Recoverable(
                "human decision rejected this transition".into(),
            )),
            HumanStatus::Pending => Err(CoreError::Recoverable(format!(
                "human decision {} is pending",
                decision.id.0
            ))),
            HumanStatus::Expired => Err(CoreError::Recoverable("human decision expired".into())),
            HumanStatus::Escalated => Err(CoreError::Recoverable(
                "human decision was escalated".into(),
            )),
        }
    }

    fn completion_evidence_generation(&self, run_id: RunId) -> CoreResult<u64> {
        let scope = EventScope {
            project_id: None,
            run_id: Some(run_id),
            task_id: None,
        };
        let mut cursor: Option<EventCursor> = None;
        let mut generation = 0;
        loop {
            let page = self
                .db
                .events_after(scope.clone(), cursor.as_ref(), 500)
                .map_err(storage_error)?;
            let count = page.items.len();
            for event in page.items {
                if event.event_type.starts_with("task.") || event.event_type.starts_with("review.")
                {
                    generation = generation.max(event.sequence);
                }
            }
            if count < 500 {
                break;
            }
            cursor = page.next_cursor.map(EventCursor);
        }
        Ok(generation)
    }

    fn expire_if_due(&self, ctx: &SessionContext, id: HumanRequestId) -> CoreResult<bool> {
        let Some((mut decision, revision)) = self
            .db
            .load::<HumanDecision>(EntityKind::HumanRequest, &id.0.to_string())
            .map_err(storage_error)?
        else {
            return Err(CoreError::NotFound);
        };
        if decision.status != HumanStatus::Pending
            || !decision
                .expires_at
                .is_some_and(|expiry| expiry <= Utc::now())
        {
            return Ok(false);
        }
        let action = decision.on_expiry.unwrap_or(HumanExpiryAction::Expire);
        decision.status = match action {
            HumanExpiryAction::Expire => HumanStatus::Expired,
            HumanExpiryAction::Escalate => HumanStatus::Escalated,
        };
        let key = IdempotencyKey(format!("expiry:{}", id.0));
        let change = StateChange::update(
            EntityKind::HumanRequest,
            id.0.to_string(),
            scope(&decision),
            revision,
            &decision,
        )
        .map_err(storage_error)?;
        let event_type = match decision.status {
            HumanStatus::Expired => "human.expired",
            _ => "human.escalated",
        };
        match self.commit_decision_with_change(
            ctx, OP_EXPIRE, &key, &decision, &decision, change, event_type,
        ) {
            Ok(_) => Ok(true),
            Err(CoreError::Recoverable(_)) => Ok(true),
            Err(error) => Err(error),
        }
    }

    fn all_for_project(&self, project_id: ProjectId) -> CoreResult<Vec<HumanDecision>> {
        let scope = EntityScope {
            project_id: Some(project_id.0.to_string()),
            ..Default::default()
        };
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = self
                .db
                .list_entities::<HumanDecision>(
                    EntityKind::HumanRequest,
                    &scope,
                    PageRequest { limit: 500, after },
                )
                .map_err(storage_error)?;
            if page.items.is_empty() {
                break;
            }
            let count = page.items.len();
            all.extend(page.items);
            if count < 500 {
                break;
            }
            after = page.next_cursor.map(EventCursor);
        }
        Ok(all)
    }

    fn load(&self, id: HumanRequestId) -> CoreResult<(HumanDecision, i64)> {
        self.db
            .load(EntityKind::HumanRequest, &id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn prior<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
    ) -> CoreResult<Option<Mutation<HumanDecision>>> {
        self.db
            .prior_mutation(ctx, operation, key, request)
            .map_err(storage_error)
    }

    fn commit_decision<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        decision: &HumanDecision,
        event_type: &str,
    ) -> CoreResult<Mutation<HumanDecision>> {
        let change = StateChange::insert(
            EntityKind::HumanRequest,
            decision.id.0.to_string(),
            scope(decision),
            decision,
        )
        .map_err(storage_error)?;
        self.commit_decision_with_change(ctx, operation, key, request, decision, change, event_type)
    }

    fn commit_decision_with_change<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        decision: &HumanDecision,
        change: StateChange,
        event_type: &str,
    ) -> CoreResult<Mutation<HumanDecision>> {
        let scope = EventScope {
            project_id: Some(decision.project_id),
            run_id: decision.run_id,
            task_id: decision.task_id,
        };
        match self.db.commit_mutation(MutationInput {
            context: ctx,
            operation,
            idempotency_key: key,
            request,
            preconditions: Vec::new(),
            result: decision,
            changes: vec![change],
            event: NewEvent::from_context(
                ctx,
                ctx.actor_id().0.to_string(),
                scope,
                event_type,
                json!({
                    "request_id": decision.id, "status": decision.status,
                    "question": decision.question, "context": decision.context,
                    "choices": decision.choices, "resolution": decision.resolution,
                }),
            ),
        }) {
            Ok(mutation) => Ok(mutation),
            Err(PersistenceError::StateConflict(_)) => {
                if let Some(prior) = self.prior(ctx, operation, key, request)? {
                    return Ok(prior);
                }
                Err(CoreError::Recoverable(
                    "human request changed concurrently; retry".into(),
                ))
            }
            Err(error) => Err(storage_error(error)),
        }
    }
}

impl HumanTransitionGate for HumanCoordinator {
    fn ensure_allowed(
        &self,
        ctx: &SessionContext,
        transition: TransitionContext,
    ) -> CoreResult<()> {
        if transition.identity != TransitionIdentity::RunCompletion {
            return Ok(());
        }
        let policy = self.policy.human_gate_policy(transition.project_id)?;
        if !policy.is_some_and(|policy| policy.completion_approval) {
            return Ok(());
        }
        self.require_gate(
            ctx,
            transition.project_id,
            transition.run_id,
            transition.task_id,
            format!("run_completion:{}", transition.run_id.0),
            "Approve run completion?".into(),
            "The run has met its configured completion criteria.".into(),
        )
    }
}

impl ReviewHumanGate for HumanCoordinator {
    fn allow_high_severity_deferral(
        &self,
        ctx: &SessionContext,
        project_id: ProjectId,
        run_id: RunId,
        task_id: TaskId,
        finding: &ReviewFinding,
        reason: &str,
    ) -> CoreResult<()> {
        self.require_gate(
            ctx,
            project_id,
            run_id,
            Some(task_id),
            format!("review_deferral:{}:{}:{}", task_id.0, finding.id.0, full_hash(reason)),
            "Approve deferring this high-severity finding?".into(),
            format!("Finding: {}\nLocation: {}\nEvidence: {}\nProposed correction: {}\nDeferral reason: {}", finding.id.0, finding.location, finding.evidence, finding.proposed_correction, reason),
        )
    }
}

#[derive(Serialize)]
struct GateRequest<'a> {
    gate_key: &'a str,
    question: &'a str,
    context: &'a str,
}

fn validate_request(request: &HumanRequest) -> CoreResult<()> {
    if request.question.trim().is_empty() || request.context.trim().is_empty() {
        return Err(CoreError::Invalid(
            "human request needs a question and context".into(),
        ));
    }
    if request.choices.len() < 2
        || request
            .choices
            .iter()
            .any(|choice| choice.trim().is_empty())
    {
        return Err(CoreError::Invalid(
            "human request needs at least two nonempty choices".into(),
        ));
    }
    let mut unique = request.choices.clone();
    unique.sort();
    unique.dedup();
    if unique.len() != request.choices.len() {
        return Err(CoreError::Invalid(
            "human request choices must be unique".into(),
        ));
    }
    if request.expires_at.is_some() != request.on_expiry.is_some() {
        return Err(CoreError::Invalid(
            "expiry time and expiry action must be configured together".into(),
        ));
    }
    if request
        .expires_at
        .is_some_and(|expires_at| expires_at <= Utc::now())
    {
        return Err(CoreError::Invalid(
            "human request expiry must be in the future".into(),
        ));
    }
    Ok(())
}

fn scope(decision: &HumanDecision) -> EntityScope {
    EntityScope {
        project_id: Some(decision.project_id.0.to_string()),
        run_id: decision.run_id.map(|id| id.0.to_string()),
        task_id: decision.task_id.map(|id| id.0.to_string()),
        owner_id: Some(decision.requested_by.0.to_string()),
    }
}

fn gate_id(key: &str) -> HumanRequestId {
    let digest = Sha256::digest(key.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    HumanRequestId(Uuid::from_bytes(bytes))
}

fn full_hash(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn require_role(ctx: &SessionContext, role: Role) -> CoreResult<()> {
    if ctx.role() == role {
        Ok(())
    } else {
        Err(CoreError::Forbidden)
    }
}
fn require_reader(ctx: &SessionContext) -> CoreResult<()> {
    if matches!(ctx.role(), Role::Supervisor | Role::Cli) {
        Ok(())
    } else {
        Err(CoreError::Forbidden)
    }
}
fn require_resolver(ctx: &SessionContext) -> CoreResult<()> {
    require_reader(ctx)
}
fn storage_error(error: PersistenceError) -> CoreError {
    match error {
        PersistenceError::IdempotencyConflict => CoreError::IdempotencyConflict,
        PersistenceError::StateConflict(_) => {
            CoreError::Recoverable("human request changed concurrently; retry".into())
        }
        error => CoreError::Internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{EntityScope, MutationInput};
    use chrono::Duration;
    use serde::de::DeserializeOwned;
    use std::{fs, path::PathBuf};

    struct TempDb(PathBuf);
    impl TempDb {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("tesselate-f07-{}", Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir.join("state.sqlite3"))
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            if let Some(parent) = self.0.parent() {
                let _ = fs::remove_dir_all(parent);
            }
        }
    }

    struct Policy(Option<HumanGatePolicy>);
    impl ProjectHumanGatePolicy for Policy {
        fn human_gate_policy(&self, _: ProjectId) -> CoreResult<Option<HumanGatePolicy>> {
            Ok(self.0.clone())
        }
    }

    struct Workflow;
    impl super::super::tasks::ProjectWorkflowPolicy for Workflow {
        fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
            Ok(WorkflowLimits {
                max_duration_hours: 24,
                max_concurrent_assignments: 2,
                max_task_retries: 2,
                max_review_rounds: 3,
            })
        }
    }

    struct Reviews;
    impl super::super::tasks::ReviewCompletionGate for Reviews {
        fn ensure_task_complete(
            &self,
            _: TaskId,
        ) -> CoreResult<Vec<crate::persistence::StatePrecondition>> {
            Ok(Vec::new())
        }
        fn reviewed_commits_for_run(&self, _: RunId) -> CoreResult<Vec<CommitRef>> {
            Ok(Vec::new())
        }
    }

    struct Leases;
    impl super::super::tasks::AssignmentLeaseStatus for Leases {
        fn expired_lease_fence(
            &self,
            _: TaskId,
            _: ActorId,
        ) -> CoreResult<super::super::tasks::LeaseFence> {
            Err(CoreError::NotFound)
        }
    }

    fn context(role: Role) -> SessionContext {
        SessionContext::validated(SessionId::new(), ActorId::new(), role)
    }

    fn fixture(
        policy: Option<HumanGatePolicy>,
    ) -> (
        TempDb,
        Database,
        HumanCoordinator,
        Project,
        Run,
        SessionContext,
        SessionContext,
    ) {
        let temp = TempDb::new();
        let db = Database::open(&temp.0).unwrap();
        let supervisor = context(Role::Supervisor);
        let cli = context(Role::Cli);
        insert_actor(&db, &supervisor, "supervisor");
        insert_actor(&db, &cli, "cli");
        let project = Project {
            id: ProjectId::new(),
            name: "test".into(),
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Project,
            project.id.0.to_string(),
            EntityScope::default(),
            &project,
            "project.created",
        );
        let run = Run {
            id: RunId::new(),
            project_id: project.id,
            goal: "goal".into(),
            started_at: Utc::now(),
            limits: WorkflowLimits {
                max_duration_hours: 1,
                max_concurrent_assignments: 1,
                max_task_retries: 1,
                max_review_rounds: 1,
            },
            status: RunStatus::Active,
            reviewed_commits: Vec::new(),
            outstanding_integration_steps: Vec::new(),
            outcome: None,
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Run,
            run.id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                ..Default::default()
            },
            &run,
            "run.created",
        );
        let coordinator = HumanCoordinator::new(db.clone(), Arc::new(Policy(policy)));
        (temp, db, coordinator, project, run, supervisor, cli)
    }

    fn insert_actor(db: &Database, ctx: &SessionContext, name: &str) {
        let actor = Actor {
            id: ctx.actor_id(),
            display_name: name.into(),
            role: ctx.role(),
        };
        insert(
            db,
            ctx,
            EntityKind::Actor,
            actor.id.0.to_string(),
            EntityScope::default(),
            &actor,
            "actor.created",
        );
    }

    fn insert<T: Serialize + DeserializeOwned>(
        db: &Database,
        ctx: &SessionContext,
        kind: EntityKind,
        id: String,
        scope: EntityScope,
        value: &T,
        event_type: &str,
    ) {
        let change = StateChange::insert(kind, id, scope, value).unwrap();
        db.commit_mutation(MutationInput {
            context: ctx,
            operation: "test.insert",
            idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
            request: value,
            preconditions: Vec::new(),
            result: value,
            changes: vec![change],
            event: NewEvent::from_context(
                ctx,
                "test",
                EventScope {
                    project_id: None,
                    run_id: None,
                    task_id: None,
                },
                event_type,
                json!({}),
            ),
        })
        .unwrap();
    }

    fn request(
        project: ProjectId,
        run: Option<RunId>,
        expires_at: Option<chrono::DateTime<Utc>>,
        on_expiry: Option<HumanExpiryAction>,
    ) -> HumanRequest {
        HumanRequest {
            project_id: project,
            run_id: run,
            task_id: None,
            question: "Choose".into(),
            context: "Context".into(),
            choices: vec!["yes".into(), "no".into()],
            expires_at,
            on_expiry,
        }
    }

    fn transition(run: &Run, identity: TransitionIdentity) -> TransitionContext {
        TransitionContext {
            project_id: run.project_id,
            run_id: run.id,
            task_id: None,
            from: None,
            to: None,
            identity,
        }
    }

    #[test]
    fn no_configured_gate_allows_autonomous_completion() {
        let (_temp, db, coordinator, _project, run, supervisor, _) = fixture(None);
        let before = db.event_count().unwrap();
        coordinator
            .ensure_allowed(
                &supervisor,
                transition(&run, TransitionIdentity::RunCompletion),
            )
            .unwrap();
        assert_eq!(db.event_count().unwrap(), before);
    }

    #[test]
    fn completion_gate_survives_restart_and_resumes_after_approval() {
        let (temp, db, coordinator, project, run, supervisor, cli) =
            fixture(Some(HumanGatePolicy {
                completion_approval: true,
            }));
        let completion = transition(&run, TransitionIdentity::RunCompletion);
        assert!(matches!(
            coordinator.ensure_allowed(&supervisor, completion),
            Err(CoreError::Recoverable(_))
        ));
        assert!(matches!(
            coordinator.ensure_allowed(
                &supervisor,
                transition(&run, TransitionIdentity::TaskAssignment)
            ),
            Ok(())
        ));
        let pending = db
            .list_entities::<HumanDecision>(
                EntityKind::HumanRequest,
                &EntityScope {
                    project_id: Some(project.id.0.to_string()),
                    ..Default::default()
                },
                PageRequest {
                    limit: 10,
                    after: None,
                },
            )
            .unwrap()
            .items
            .remove(0);
        drop(coordinator);
        drop(db);
        let db = Database::open(&temp.0).unwrap();
        let coordinator = HumanCoordinator::new(
            db.clone(),
            Arc::new(Policy(Some(HumanGatePolicy {
                completion_approval: true,
            }))),
        );
        let page = coordinator
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(page.items[0].id, pending.id);
        let resolved = coordinator
            .resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: pending.id,
                    choice: "approve".into(),
                    reason: "Reviewed result".into(),
                },
                IdempotencyKey("approve".into()),
            )
            .unwrap();
        assert_eq!(resolved.value.status, HumanStatus::Resolved);
        coordinator.ensure_allowed(&supervisor, completion).unwrap();
    }

    #[test]
    fn rejected_completion_gate_reopens_after_corrective_task_change() {
        let (_temp, db, human, project, run, supervisor, cli) = fixture(Some(HumanGatePolicy {
            completion_approval: true,
        }));
        let engine = super::super::tasks::TaskEngine::new(
            db.clone(),
            Arc::new(Workflow),
            Arc::new(Reviews),
            Arc::new(human),
            Arc::new(Leases),
        );
        let completion = CompleteRun {
            run_id: run.id,
            outstanding_integration_steps: Vec::new(),
        };
        let key = IdempotencyKey("complete-after-gate".into());
        assert!(matches!(
            engine.complete_run(&supervisor, completion.clone(), key.clone()),
            Err(CoreError::Recoverable(_))
        ));
        let first = db
            .list_entities::<HumanDecision>(
                EntityKind::HumanRequest,
                &EntityScope {
                    project_id: Some(project.id.0.to_string()),
                    ..Default::default()
                },
                PageRequest {
                    limit: 10,
                    after: None,
                },
            )
            .unwrap()
            .items
            .remove(0);
        let human = HumanCoordinator::new(
            db.clone(),
            Arc::new(Policy(Some(HumanGatePolicy {
                completion_approval: true,
            }))),
        );
        human
            .resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: first.id,
                    choice: "reject".into(),
                    reason: "more work is needed".into(),
                },
                IdempotencyKey("reject-first".into()),
            )
            .unwrap();
        assert!(matches!(
            engine.complete_run(&supervisor, completion.clone(), key.clone()),
            Err(CoreError::Recoverable(_))
        ));
        let unchanged = human
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(unchanged.items.len(), 1);
        assert_eq!(unchanged.items[0].id, first.id);

        let (run_record, run_revision) = db
            .load::<Run>(EntityKind::Run, &run.id.0.to_string())
            .unwrap()
            .unwrap();
        let task_id = TaskId::new();
        let task = Task {
            id: task_id,
            run_id: run.id,
            title: "corrective task".into(),
            goal: "complete the requested fix".into(),
            status: TaskStatus::Completed,
            owner: None,
            progress: None,
            attempts: 1,
            submitted_commits: Vec::new(),
            result: Some("fixed".into()),
        };
        let record = super::super::tasks::TaskRecord {
            task: task.clone(),
            dependencies: Vec::new(),
            waiting_on_dependencies: false,
            final_disposition: Some(FinalTaskDisposition {
                task_id,
                outcome: FinalTaskOutcome::Completed,
                reason: "corrective work complete".into(),
            }),
        };
        let changes = vec![
            StateChange::update(
                EntityKind::Run,
                run.id.0.to_string(),
                EntityScope {
                    project_id: Some(project.id.0.to_string()),
                    run_id: Some(run.id.0.to_string()),
                    ..Default::default()
                },
                run_revision,
                &run_record,
            )
            .unwrap(),
            StateChange::insert(
                EntityKind::Task,
                task_id.0.to_string(),
                EntityScope {
                    project_id: Some(project.id.0.to_string()),
                    run_id: Some(run.id.0.to_string()),
                    task_id: Some(task_id.0.to_string()),
                    ..Default::default()
                },
                &record,
            )
            .unwrap(),
        ];
        db.commit_mutation(MutationInput {
            context: &supervisor,
            operation: "test.corrective_work",
            idempotency_key: &IdempotencyKey("corrective-work".into()),
            request: &task_id,
            preconditions: Vec::new(),
            result: &task,
            changes,
            event: NewEvent::from_context(
                &supervisor,
                "supervisor",
                EventScope {
                    project_id: Some(project.id),
                    run_id: Some(run.id),
                    task_id: Some(task_id),
                },
                "task.disposition",
                json!({"outcome":"completed","reason":"corrective work complete"}),
            ),
        })
        .unwrap();

        assert!(matches!(
            engine.complete_run(&supervisor, completion.clone(), key.clone()),
            Err(CoreError::Recoverable(_))
        ));
        let decisions = human
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(decisions.items.len(), 2);
        let next = decisions
            .items
            .iter()
            .find(|decision| decision.id != first.id)
            .unwrap();
        human
            .resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: next.id,
                    choice: "approve".into(),
                    reason: "corrective work reviewed".into(),
                },
                IdempotencyKey("approve-second".into()),
            )
            .unwrap();
        let completed = engine.complete_run(&supervisor, completion, key).unwrap();
        assert_eq!(completed.value.status, RunStatus::Completed);
        let history = human
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(history.items.len(), 2);
        assert!(history.items.iter().any(|decision| decision.id == first.id
            && decision.resolution.as_ref().unwrap().choice == "reject"));
        assert!(history.items.iter().any(|decision| decision.id == next.id
            && decision.resolution.as_ref().unwrap().choice == "approve"));
    }

    #[test]
    fn rejects_bad_choices_and_unauthorized_resolution() {
        let (_temp, _db, coordinator, project, run, supervisor, cli) = fixture(None);
        let mutation = coordinator
            .request_human(
                &supervisor,
                request(project.id, Some(run.id), None, None),
                IdempotencyKey("request".into()),
            )
            .unwrap();
        assert_eq!(
            coordinator
                .resolve_human(
                    &cli,
                    ResolveHumanRequest {
                        request_id: mutation.value.id,
                        choice: "maybe".into(),
                        reason: "reason".into()
                    },
                    IdempotencyKey("bad-choice".into())
                )
                .unwrap_err()
                .to_string(),
            "invalid request: choice is not one of this request's choices"
        );
        let coder = context(Role::Coder);
        assert!(matches!(
            coordinator.resolve_human(
                &coder,
                ResolveHumanRequest {
                    request_id: mutation.value.id,
                    choice: "yes".into(),
                    reason: "reason".into()
                },
                IdempotencyKey("unauthorized".into())
            ),
            Err(CoreError::Forbidden)
        ));
    }

    #[test]
    fn resolutions_are_idempotent_and_expiry_is_durable() {
        let (_temp, db, coordinator, project, run, supervisor, cli) = fixture(None);
        let created = coordinator
            .request_human(
                &supervisor,
                request(project.id, Some(run.id), None, None),
                IdempotencyKey("request".into()),
            )
            .unwrap();
        let resolution = ResolveHumanRequest {
            request_id: created.value.id,
            choice: "yes".into(),
            reason: "approved".into(),
        };
        let first = coordinator
            .resolve_human(&cli, resolution.clone(), IdempotencyKey("resolve".into()))
            .unwrap();
        assert!(!first.replayed);
        assert!(
            coordinator
                .resolve_human(&cli, resolution.clone(), IdempotencyKey("resolve".into()))
                .unwrap()
                .replayed
        );
        assert!(coordinator
            .resolve_human(&cli, resolution, IdempotencyKey("resolve-again".into()))
            .is_ok());
        assert!(matches!(
            coordinator.resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: created.value.id,
                    choice: "no".into(),
                    reason: "changed answer".into(),
                },
                IdempotencyKey("resolve-again".into()),
            ),
            Err(CoreError::IdempotencyConflict)
        ));
        assert!(matches!(
            coordinator.resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: created.value.id,
                    choice: "no".into(),
                    reason: "changed answer".into(),
                },
                IdempotencyKey("conflicting-resolution".into()),
            ),
            Err(CoreError::Invalid(_))
        ));
        let expired = coordinator
            .request_human(
                &supervisor,
                request(
                    project.id,
                    Some(run.id),
                    Some(Utc::now() + Duration::milliseconds(1)),
                    Some(HumanExpiryAction::Escalate),
                ),
                IdempotencyKey("expires".into()),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let listed = coordinator
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        assert_eq!(
            listed
                .items
                .iter()
                .find(|item| item.id == expired.value.id)
                .unwrap()
                .status,
            HumanStatus::Escalated
        );
        let events = db
            .events_after(
                EventScope {
                    project_id: Some(project.id),
                    run_id: None,
                    task_id: None,
                },
                None,
                500,
            )
            .unwrap();
        let resolution_event = events
            .items
            .iter()
            .find(|event| event.event_type == "human.resolved")
            .unwrap();
        assert_eq!(resolution_event.actor.id, cli.actor_id());
        assert_eq!(resolution_event.payload["resolution"]["reason"], "approved");
        assert!(db.event_count().unwrap() >= 6);
    }

    #[test]
    fn review_deferral_creates_durable_task_scoped_gate_and_resumes() {
        let (_temp, db, coordinator, project, run, supervisor, cli) = fixture(None);
        let task_id = TaskId::new();
        let task = Task {
            id: task_id,
            run_id: run.id,
            title: "test".into(),
            goal: "goal".into(),
            status: TaskStatus::InReview,
            owner: None,
            progress: None,
            attempts: 0,
            submitted_commits: Vec::new(),
            result: None,
        };
        let record = super::super::tasks::TaskRecord {
            task,
            dependencies: Vec::new(),
            waiting_on_dependencies: false,
            final_disposition: None,
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Task,
            task_id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                run_id: Some(run.id.0.to_string()),
                task_id: Some(task_id.0.to_string()),
                ..Default::default()
            },
            &record,
            "task.created",
        );
        let finding = ReviewFinding {
            id: FindingId::new(),
            severity: FindingSeverity::High,
            location: "src/main.rs:1".into(),
            evidence: "issue".into(),
            proposed_correction: "fix it".into(),
        };
        assert!(matches!(
            coordinator.allow_high_severity_deferral(
                &supervisor,
                project.id,
                run.id,
                task_id,
                &finding,
                "defer for next cycle"
            ),
            Err(CoreError::Recoverable(_))
        ));
        let page = coordinator
            .list_human_requests(
                &cli,
                ListHumanRequests {
                    project_id: project.id,
                    page: PageRequest {
                        limit: 10,
                        after: None,
                    },
                },
            )
            .unwrap();
        let gate = page.items.first().unwrap();
        assert_eq!(gate.task_id, Some(task_id));
        assert_eq!(gate.status, HumanStatus::Pending);
        drop(coordinator);
        drop(db);
        let reopened = Database::open(&_temp.0).unwrap();
        let coordinator = HumanCoordinator::new(reopened, Arc::new(Policy(None)));
        coordinator
            .resolve_human(
                &cli,
                ResolveHumanRequest {
                    request_id: gate.id,
                    choice: "approve".into(),
                    reason: "accepted".into(),
                },
                IdempotencyKey("resolve-gate".into()),
            )
            .unwrap();
        coordinator
            .allow_high_severity_deferral(
                &supervisor,
                project.id,
                run.id,
                task_id,
                &finding,
                "defer for next cycle",
            )
            .unwrap();
    }
}
