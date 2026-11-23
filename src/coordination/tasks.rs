//! Durable run and task transitions. Policy checks live here; all resulting state is committed
//! through the F02 transaction boundary.

use crate::{
    core::*,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
        StatePrecondition,
    },
};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

const PAGE_SIZE: u32 = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskRecord {
    pub task: Task,
    pub dependencies: Vec<TaskId>,
    pub waiting_on_dependencies: bool,
    pub final_disposition: Option<FinalTaskDisposition>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentStatus {
    Active,
    Released,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssignmentRecord {
    pub id: String,
    pub project_id: ProjectId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub assignee: ActorId,
    pub assigned_by: ActorId,
    pub attempt: u32,
    pub status: AssignmentStatus,
    pub assigned_at: chrono::DateTime<Utc>,
    pub ended_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewTaskTransition {
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransitionIdentity {
    RunCreation,
    TaskCreation,
    TaskClaim,
    TaskAssignment,
    TaskRelease,
    TaskProgress,
    TaskSubmission,
    ReviewStatus,
    FinalDisposition,
    RunCompletion,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "entity", content = "status")]
pub enum LifecycleState {
    Run(RunStatus),
    Task(TaskStatus),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransitionContext {
    pub project_id: ProjectId,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub from: Option<LifecycleState>,
    pub to: Option<LifecycleState>,
    pub identity: TransitionIdentity,
}

/// F01 supplies validated policy from project configuration; callers cannot put limits in CreateRun.
pub trait ProjectWorkflowPolicy: Send + Sync {
    fn workflow_limits(&self, project_id: ProjectId) -> CoreResult<WorkflowLimits>;
}

/// F06 decides whether a submitted task or run has satisfied review policy and derives reviewed SHAs.
pub trait ReviewCompletionGate: Send + Sync {
    fn ensure_task_complete(&self, task_id: TaskId) -> CoreResult<Vec<StatePrecondition>>;
    fn reviewed_commits_for_run(&self, run_id: RunId) -> CoreResult<Vec<CommitRef>>;
}

/// F07 owns policy-governed human pauses; this hook runs before the task engine commits a transition.
pub trait HumanTransitionGate: Send + Sync {
    fn ensure_allowed(&self, ctx: &SessionContext, transition: TransitionContext)
        -> CoreResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseFence {
    pub task_id: TaskId,
    pub owner_id: ActorId,
    pub condition: StatePrecondition,
}

/// F05 supplies an expired lease's durable revision so renewals can fence a reassignment.
pub trait AssignmentLeaseStatus: Send + Sync {
    fn expired_lease_fence(&self, task_id: TaskId, owner: ActorId) -> CoreResult<LeaseFence>;
}

pub struct TaskEngine {
    db: Database,
    policy: Arc<dyn ProjectWorkflowPolicy>,
    reviews: Arc<dyn ReviewCompletionGate>,
    human: Arc<dyn HumanTransitionGate>,
    leases: Arc<dyn AssignmentLeaseStatus>,
}

impl TaskEngine {
    pub fn new(
        db: Database,
        policy: Arc<dyn ProjectWorkflowPolicy>,
        reviews: Arc<dyn ReviewCompletionGate>,
        human: Arc<dyn HumanTransitionGate>,
        leases: Arc<dyn AssignmentLeaseStatus>,
    ) -> Self {
        Self {
            db,
            policy,
            reviews,
            human,
            leases,
        }
    }

    fn ensure_transition(
        &self,
        ctx: &SessionContext,
        project_id: ProjectId,
        run_id: RunId,
        task_id: Option<TaskId>,
        from: Option<LifecycleState>,
        to: Option<LifecycleState>,
        identity: TransitionIdentity,
    ) -> CoreResult<()> {
        self.human.ensure_allowed(
            ctx,
            TransitionContext {
                project_id,
                run_id,
                task_id,
                from,
                to,
                identity,
            },
        )
    }

    pub fn create_run(
        &self,
        ctx: &SessionContext,
        request: CreateRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        const OP: &str = "runs.create";
        if let Some(prior) = self.prior_run(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        if request.goal.trim().is_empty() {
            return Err(CoreError::Invalid("run goal is required".into()));
        }
        self.db
            .load::<Project>(EntityKind::Project, &request.project_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let limits = self.policy.workflow_limits(request.project_id)?;
        validate_limits(&limits)?;
        let run = Run {
            id: RunId::new(),
            project_id: request.project_id,
            goal: request.goal.clone(),
            started_at: Utc::now(),
            limits,
            status: RunStatus::Active,
            reviewed_commits: Vec::new(),
            outstanding_integration_steps: Vec::new(),
            outcome: None,
        };
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            None,
            None,
            Some(LifecycleState::Run(RunStatus::Active)),
            TransitionIdentity::RunCreation,
        )?;
        let change = StateChange::insert(
            EntityKind::Run,
            run.id.0.to_string(),
            scope_for_run(&run),
            &run,
        )
        .map_err(storage_error)?;
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            None,
            "run.created",
            json!({"goal":run.goal}),
        );
        self.commit_run(ctx, OP, &key, &request, &run, vec![change], event)
    }

    pub fn list_tasks(
        &self,
        _ctx: &SessionContext,
        run_id: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>> {
        if self
            .db
            .load::<Run>(EntityKind::Run, &run_id.0.to_string())
            .map_err(storage_error)?
            .is_none()
        {
            return Err(CoreError::NotFound);
        }
        let records = self
            .db
            .list_entities::<TaskRecord>(
                EntityKind::Task,
                &EntityScope {
                    run_id: Some(run_id.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(storage_error)?;
        Ok(Page {
            items: records
                .items
                .into_iter()
                .map(|record| record.task)
                .collect(),
            next_cursor: records.next_cursor,
        })
    }

    pub fn available_work(
        &self,
        ctx: &SessionContext,
        project_id: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>> {
        if !matches!(ctx.role(), Role::Coder | Role::Supervisor) {
            return Err(CoreError::Forbidden);
        }
        self.db
            .load::<Project>(EntityKind::Project, &project_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let records = self
            .db
            .list_entities::<TaskRecord>(
                EntityKind::Task,
                &EntityScope {
                    project_id: Some(project_id.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(storage_error)?;
        let mut runs = std::collections::HashMap::<RunId, Run>::new();
        for run_record in self.all_entities::<Run>(
            EntityKind::Run,
            EntityScope {
                project_id: Some(project_id.0.to_string()),
                ..Default::default()
            },
        )? {
            let (mut run, revision) = self.load_run(run_record.id)?;
            if run.status == RunStatus::Active && duration_expired(&run) {
                match self.stop_for_limit(ctx, &run, revision, "run duration limit reached") {
                    Ok(()) => {
                        run.status = RunStatus::Stopped;
                        run.outcome = Some(RunOutcome {
                            reason: "run duration limit reached".into(),
                            remaining_task_ids: self
                                .all_tasks(run.id)?
                                .into_iter()
                                .filter(|record| record.task.status != TaskStatus::Completed)
                                .map(|record| record.task.id)
                                .collect(),
                        });
                    }
                    Err(CoreError::Recoverable(_)) => {
                        let (latest, _) = self.load_run(run.id)?;
                        if latest.status == RunStatus::Active && duration_expired(&latest) {
                            return Err(CoreError::Recoverable(
                                "run expiry changed concurrently; retry work discovery".into(),
                            ));
                        }
                        run = latest;
                    }
                    Err(error) => return Err(error),
                }
            }
            runs.insert(run.id, run);
        }
        let all = self.all_entities::<TaskRecord>(
            EntityKind::Task,
            EntityScope {
                project_id: Some(project_id.0.to_string()),
                ..Default::default()
            },
        )?;
        let mut active_counts = std::collections::HashMap::<RunId, u32>::new();
        for record in &all {
            if matches!(
                record.task.status,
                TaskStatus::Assigned | TaskStatus::Coding
            ) {
                *active_counts.entry(record.task.run_id).or_default() += 1;
            }
        }
        let items = records
            .items
            .into_iter()
            .filter_map(|record| {
                let run = runs.get(&record.task.run_id)?;
                let active = active_counts.get(&run.id).copied().unwrap_or_default();
                (run.status == RunStatus::Active
                    && !duration_expired(run)
                    && active < run.limits.max_concurrent_assignments
                    && record.task.status == TaskStatus::Ready)
                    .then_some(record.task)
            })
            .collect();
        Ok(Page {
            items,
            next_cursor: records.next_cursor,
        })
    }

    pub fn create_task(
        &self,
        ctx: &SessionContext,
        request: CreateTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.create_task_inner(
            ctx,
            "tasks.create",
            &request,
            request.run_id,
            request.title.clone(),
            request.goal.clone(),
            Vec::new(),
            key,
        )
    }

    pub fn create_task_with_dependencies(
        &self,
        ctx: &SessionContext,
        request: CreateTaskWithDependencies,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.create_task_inner(
            ctx,
            "tasks.create_with_dependencies",
            &request,
            request.run_id,
            request.title.clone(),
            request.goal.clone(),
            request.dependencies.clone(),
            key,
        )
    }

    fn create_task_inner<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        request: &Q,
        run_id: RunId,
        title: String,
        goal: String,
        dependencies: Vec<TaskId>,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        if let Some(prior) = self.prior_task(ctx, operation, &key, request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        if title.trim().is_empty() || goal.trim().is_empty() {
            return Err(CoreError::Invalid(
                "task title and goal are required".into(),
            ));
        }
        let (mut run, run_revision) = self.load_run(run_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let mut waiting = false;
        for dependency in &dependencies {
            let prerequisite = self.load_task(*dependency)?.0;
            if prerequisite.task.run_id != run.id {
                return Err(CoreError::Invalid(
                    "task dependencies must belong to the same run".into(),
                ));
            }
            if matches!(
                prerequisite.task.status,
                TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                return Err(CoreError::Invalid(
                    "cannot depend on a failed or cancelled task".into(),
                ));
            }
            waiting |= prerequisite.task.status != TaskStatus::Completed;
        }
        let task = Task {
            id: TaskId::new(),
            run_id: run.id,
            title,
            goal,
            status: if waiting {
                TaskStatus::Blocked
            } else {
                TaskStatus::Ready
            },
            owner: None,
            progress: None,
            attempts: 0,
            submitted_commits: Vec::new(),
            result: None,
        };
        let record = TaskRecord {
            task: task.clone(),
            dependencies: dependencies.clone(),
            waiting_on_dependencies: waiting,
            final_disposition: None,
        };
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(task.id),
            None,
            Some(LifecycleState::Task(task.status)),
            TransitionIdentity::TaskCreation,
        )?;
        let task_change = StateChange::insert(
            EntityKind::Task,
            task.id.0.to_string(),
            scope_for_task(&run, &task),
            &record,
        )
        .map_err(storage_error)?
        .with_dependencies(dependencies);
        let changes = vec![self.bump_run(&mut run, run_revision)?, task_change];
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(task.id),
            "task.created",
            json!({"title":task.title,"dependency_count":record.dependencies.len()}),
        );
        self.commit_task(ctx, operation, &key, request, &task, changes, event)
    }

    pub fn assign_task(
        &self,
        ctx: &SessionContext,
        request: AssignTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.assign";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        let actor = self
            .db
            .load::<Actor>(EntityKind::Actor, &request.assignee.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?
            .0;
        if actor.role != Role::Coder {
            return Err(CoreError::Forbidden);
        }
        self.assign_to(ctx, OP, &request, request.task_id, request.assignee, key)
    }

    pub fn claim_task(
        &self,
        ctx: &SessionContext,
        request: ClaimTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.claim";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Coder)?;
        let (mut run, run_revision) = self.load_task_run(request.task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(request.task_id)?;
        if record.task.status == TaskStatus::Assigned && record.task.owner == Some(ctx.actor_id()) {
            self.ensure_transition(
                ctx,
                run.project_id,
                run.id,
                Some(request.task_id),
                Some(LifecycleState::Task(TaskStatus::Assigned)),
                Some(LifecycleState::Task(TaskStatus::Coding)),
                TransitionIdentity::TaskClaim,
            )?;
            record.task.status = TaskStatus::Coding;
            record.task.progress.get_or_insert_with(|| "claimed".into());
            let changes = vec![
                self.bump_run(&mut run, run_revision)?,
                self.task_update(&record, task_revision, &run)?,
            ];
            let event = event_for(
                ctx,
                Some(run.project_id),
                Some(run.id),
                Some(request.task_id),
                "task.claimed",
                json!({"status":"coding"}),
            );
            return self.commit_task(ctx, OP, &key, &request, &record.task, changes, event);
        }
        if !matches!(
            record.task.status,
            TaskStatus::Ready | TaskStatus::RevisionRequired
        ) || record.waiting_on_dependencies
        {
            return Err(CoreError::Recoverable(
                "task is not available to claim".into(),
            ));
        }
        self.assign_ready_task(
            ctx,
            OP,
            &request,
            request.task_id,
            ctx.actor_id(),
            key,
            run,
            run_revision,
            record,
            task_revision,
            None,
        )
    }

    fn assign_to<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        request: &Q,
        task_id: TaskId,
        assignee: ActorId,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        let (mut run, run_revision) = self.load_task_run(task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (record, task_revision) = self.load_task(task_id)?;
        if record.waiting_on_dependencies {
            return Err(CoreError::Recoverable(
                "task is waiting for dependencies".into(),
            ));
        }
        if record.task.status == TaskStatus::Ready {
            return self.assign_ready_task(
                ctx,
                operation,
                request,
                task_id,
                assignee,
                key,
                run,
                run_revision,
                record,
                task_revision,
                None,
            );
        }
        if record.task.status == TaskStatus::RevisionRequired {
            return self.assign_ready_task(
                ctx,
                operation,
                request,
                task_id,
                assignee,
                key,
                run,
                run_revision,
                record,
                task_revision,
                None,
            );
        }
        let old_owner = record
            .task
            .owner
            .ok_or(CoreError::Invalid("task has no current owner".into()))?;
        if !matches!(
            record.task.status,
            TaskStatus::Assigned | TaskStatus::Coding
        ) {
            return Err(CoreError::Recoverable(
                "task is already assigned to an active session".into(),
            ));
        }
        let fence = self.leases.expired_lease_fence(task_id, old_owner)?;
        if fence.task_id != task_id || fence.owner_id != old_owner {
            return Err(CoreError::Internal(
                "lease hook returned a fence for another assignment".into(),
            ));
        }
        self.assign_ready_task(
            ctx,
            operation,
            request,
            task_id,
            assignee,
            key,
            run,
            run_revision,
            record,
            task_revision,
            Some(fence),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn assign_ready_task<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        request: &Q,
        task_id: TaskId,
        assignee: ActorId,
        key: IdempotencyKey,
        mut run: Run,
        run_revision: i64,
        mut record: TaskRecord,
        task_revision: i64,
        lease_fence: Option<LeaseFence>,
    ) -> CoreResult<Mutation<Task>> {
        let was_active = matches!(
            record.task.status,
            TaskStatus::Assigned | TaskStatus::Coding
        );
        let mut active = self.active_assignment_count(run.id)?;
        if was_active {
            active = active.saturating_sub(1);
        }
        if active >= run.limits.max_concurrent_assignments {
            return Err(CoreError::Recoverable(
                "concurrent assignment limit reached".into(),
            ));
        }
        if record.task.attempts > run.limits.max_task_retries {
            self.stop_for_limit(
                ctx,
                &run,
                run_revision,
                &format!("task retry limit reached for {}", task_id.0),
            )?;
            return Err(CoreError::Recoverable(
                "task retry limit reached; run stopped".into(),
            ));
        }
        let (from, previous_assignment) = if was_active {
            let current = self.current_assignment(task_id)?.ok_or_else(|| {
                CoreError::Internal("active task has no assignment record".into())
            })?;
            self.ensure_no_workspace_mutation(&current.0)?;
            (Some(record.task.status), Some(current))
        } else {
            (Some(record.task.status), None)
        };
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(task_id),
            from.map(LifecycleState::Task),
            Some(LifecycleState::Task(TaskStatus::Assigned)),
            TransitionIdentity::TaskAssignment,
        )?;
        record.task.status = TaskStatus::Assigned;
        record.task.owner = Some(assignee);
        record.task.attempts += 1;
        record.task.progress = None;
        let assignment = AssignmentRecord {
            id: Uuid::new_v4().to_string(),
            project_id: run.project_id,
            run_id: run.id,
            task_id,
            assignee,
            assigned_by: ctx.actor_id(),
            attempt: record.task.attempts,
            status: AssignmentStatus::Active,
            assigned_at: Utc::now(),
            ended_reason: None,
        };
        let mut changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
        ];
        if let Some((old_assignment, revision)) = previous_assignment {
            let mut ended = old_assignment;
            ended.status = AssignmentStatus::Superseded;
            ended.ended_reason = Some("previous owner lease expired".into());
            changes.push(
                StateChange::update(
                    EntityKind::Assignment,
                    ended.id.clone(),
                    assignment_scope(&ended),
                    revision,
                    &ended,
                )
                .map_err(storage_error)?,
            );
        }
        changes.push(
            StateChange::insert(
                EntityKind::Assignment,
                assignment.id.clone(),
                assignment_scope(&assignment),
                &assignment,
            )
            .map_err(storage_error)?,
        );
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(task_id),
            "task.assigned",
            json!({"assignee":assignee,"attempt":record.task.attempts,"workspace_request_required":true}),
        );
        if let Some(fence) = lease_fence {
            self.commit_task_with_preconditions(
                ctx,
                operation,
                &key,
                request,
                &record.task,
                changes,
                event,
                vec![fence.condition],
                Some((fence.task_id, fence.owner_id)),
            )
        } else {
            self.commit_task(ctx, operation, &key, request, &record.task, changes, event)
        }
    }

    pub fn release_task(
        &self,
        ctx: &SessionContext,
        request: ReleaseTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.release";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        let is_supervisor = ctx.role() == Role::Supervisor;
        if !is_supervisor && ctx.role() != Role::Coder {
            return Err(CoreError::Forbidden);
        }
        let (mut run, run_revision) = self.load_task_run(request.task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(request.task_id)?;
        if !matches!(
            record.task.status,
            TaskStatus::Assigned | TaskStatus::Coding
        ) {
            return Err(CoreError::Invalid(
                "only assigned or coding tasks can be released".into(),
            ));
        }
        if !is_supervisor && record.task.owner != Some(ctx.actor_id()) {
            return Err(CoreError::Forbidden);
        }
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            Some(LifecycleState::Task(record.task.status)),
            Some(LifecycleState::Task(TaskStatus::Ready)),
            TransitionIdentity::TaskRelease,
        )?;
        let (mut assignment, assignment_revision) = self
            .current_assignment(request.task_id)?
            .ok_or_else(|| CoreError::Internal("active task has no assignment record".into()))?;
        self.ensure_no_workspace_mutation(&assignment)?;
        assignment.status = AssignmentStatus::Released;
        assignment.ended_reason = Some(request.reason.clone());
        record.task.status = if record.waiting_on_dependencies {
            TaskStatus::Blocked
        } else {
            TaskStatus::Ready
        };
        record.task.owner = None;
        record.task.progress = None;
        let changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
            StateChange::update(
                EntityKind::Assignment,
                assignment.id.clone(),
                assignment_scope(&assignment),
                assignment_revision,
                &assignment,
            )
            .map_err(storage_error)?,
        ];
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(request.task_id),
            "task.released",
            json!({"reason":request.reason}),
        );
        self.commit_task(ctx, OP, &key, &request, &record.task, changes, event)
    }

    pub fn update_progress(
        &self,
        ctx: &SessionContext,
        request: UpdateProgress,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.progress";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Coder)?;
        let (mut run, run_revision) = self.load_task_run(request.task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(request.task_id)?;
        if record.task.owner != Some(ctx.actor_id())
            || !matches!(
                record.task.status,
                TaskStatus::Assigned | TaskStatus::Coding
            )
        {
            return Err(CoreError::Forbidden);
        }
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            Some(LifecycleState::Task(record.task.status)),
            Some(LifecycleState::Task(TaskStatus::Coding)),
            TransitionIdentity::TaskProgress,
        )?;
        record.task.status = TaskStatus::Coding;
        record.task.progress = Some(request.progress.clone());
        let changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
        ];
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(request.task_id),
            "task.progress",
            json!({"progress":request.progress}),
        );
        self.commit_task(ctx, OP, &key, &request, &record.task, changes, event)
    }

    pub fn submit_task(
        &self,
        ctx: &SessionContext,
        request: SubmitTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.submit";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Coder)?;
        if request.commits.is_empty() {
            return Err(CoreError::Invalid(
                "submission must include at least one commit ID".into(),
            ));
        }
        let (mut run, run_revision) = self.load_task_run(request.task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(request.task_id)?;
        if record.task.owner != Some(ctx.actor_id()) || record.task.status != TaskStatus::Coding {
            return Err(CoreError::Forbidden);
        }
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            Some(LifecycleState::Task(TaskStatus::Coding)),
            Some(LifecycleState::Task(TaskStatus::Submitted)),
            TransitionIdentity::TaskSubmission,
        )?;
        record.task.status = TaskStatus::Submitted;
        record.task.submitted_commits = request.commits.clone();
        record.task.result = Some(request.summary.clone());
        let mut changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
        ];
        if let Some((mut assignment, revision)) = self.current_assignment(request.task_id)? {
            self.ensure_no_workspace_mutation(&assignment)?;
            assignment.status = AssignmentStatus::Released;
            assignment.ended_reason = Some("task submitted".into());
            changes.push(
                StateChange::update(
                    EntityKind::Assignment,
                    assignment.id.clone(),
                    assignment_scope(&assignment),
                    revision,
                    &assignment,
                )
                .map_err(storage_error)?,
            );
        }
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(request.task_id),
            "task.submitted",
            json!({"commits":request.commits,"summary":request.summary}),
        );
        self.commit_task(ctx, OP, &key, &request, &record.task, changes, event)
    }

    /// F06 uses this prepare hook to commit review records and a legal task transition atomically.
    #[allow(dead_code)] // F06 integration uses this hook once its review coordinator is wired.
    pub(crate) fn prepare_review_transition(
        &self,
        ctx: &SessionContext,
        task_id: TaskId,
        status: TaskStatus,
        reason: &str,
    ) -> CoreResult<(Task, Vec<StateChange>)> {
        if !matches!(ctx.role(), Role::Supervisor | Role::Reviewer) {
            return Err(CoreError::Forbidden);
        }
        if !matches!(status, TaskStatus::InReview | TaskStatus::RevisionRequired) {
            return Err(CoreError::Invalid(
                "review hook accepts only in_review or revision_required".into(),
            ));
        }
        let (mut run, run_revision) = self.load_task_run(task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(task_id)?;
        if !transition_allowed(record.task.status, status) {
            return Err(CoreError::Invalid("illegal task transition".into()));
        }
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(task_id),
            Some(LifecycleState::Task(record.task.status)),
            Some(LifecycleState::Task(status)),
            TransitionIdentity::ReviewStatus,
        )?;
        record.task.status = status;
        record.task.result = Some(reason.to_owned());
        let changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
        ];
        Ok((record.task, changes))
    }

    pub fn record_task_disposition(
        &self,
        ctx: &SessionContext,
        request: FinalTaskDisposition,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "tasks.disposition";
        if let Some(prior) = self.prior_task(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        let (mut run, run_revision) = self.load_task_run(request.task_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let (mut record, task_revision) = self.load_task(request.task_id)?;
        let target = match request.outcome {
            FinalTaskOutcome::Completed => TaskStatus::Completed,
            FinalTaskOutcome::Blocked => TaskStatus::Blocked,
            FinalTaskOutcome::Failed => TaskStatus::Failed,
            FinalTaskOutcome::Cancelled => TaskStatus::Cancelled,
        };
        if !transition_allowed(record.task.status, target) {
            return Err(CoreError::Invalid("illegal final task disposition".into()));
        }
        let review_guards = if target == TaskStatus::Completed {
            self.reviews.ensure_task_complete(request.task_id)?
        } else {
            Vec::new()
        };
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            Some(LifecycleState::Task(record.task.status)),
            Some(LifecycleState::Task(target)),
            TransitionIdentity::FinalDisposition,
        )?;
        record.task.status = target;
        record.task.result = Some(request.reason.clone());
        record.final_disposition = Some(request.clone());
        let mut changes = vec![
            self.bump_run(&mut run, run_revision)?,
            self.task_update(&record, task_revision, &run)?,
        ];
        if let Some((mut assignment, revision)) = self.current_assignment(request.task_id)? {
            self.ensure_no_workspace_mutation(&assignment)?;
            assignment.status = AssignmentStatus::Released;
            assignment.ended_reason = Some(format!("task disposition: {:?}", request.outcome));
            changes.push(
                StateChange::update(
                    EntityKind::Assignment,
                    assignment.id.clone(),
                    assignment_scope(&assignment),
                    revision,
                    &assignment,
                )
                .map_err(storage_error)?,
            );
        }
        if target == TaskStatus::Completed {
            changes.extend(self.unblock_dependents(&run, request.task_id)?);
        }
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            Some(request.task_id),
            "task.disposition",
            json!({"outcome":request.outcome,"reason":request.reason}),
        );
        self.commit_task_with_preconditions(
            ctx,
            OP,
            &key,
            &request,
            &record.task,
            changes,
            event,
            review_guards,
            None,
        )
    }

    pub fn complete_run(
        &self,
        ctx: &SessionContext,
        request: CompleteRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        const OP: &str = "runs.complete";
        if let Some(prior) = self.prior_run(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        let (mut run, run_revision) = self.load_run(request.run_id)?;
        self.ensure_run_active(ctx, &mut run, run_revision)?;
        let tasks = self.all_tasks(run.id)?;
        if tasks
            .iter()
            .any(|record| record.task.status != TaskStatus::Completed)
        {
            return Err(CoreError::Recoverable(
                "all tasks must be completed before the run".into(),
            ));
        }
        self.ensure_transition(
            ctx,
            run.project_id,
            run.id,
            None,
            Some(LifecycleState::Run(RunStatus::Active)),
            Some(LifecycleState::Run(RunStatus::Completed)),
            TransitionIdentity::RunCompletion,
        )?;
        run.reviewed_commits = self.reviews.reviewed_commits_for_run(run.id)?;
        run.outstanding_integration_steps = request.outstanding_integration_steps.clone();
        run.status = RunStatus::Completed;
        run.outcome = None;
        let changes = vec![self.bump_run(&mut run, run_revision)?];
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            None,
            "run.completed",
            json!({"reviewed_commits":run.reviewed_commits,"outstanding_integration_steps":run.outstanding_integration_steps}),
        );
        self.commit_run(ctx, OP, &key, &request, &run, changes, event)
    }

    fn ensure_run_active(
        &self,
        ctx: &SessionContext,
        run: &mut Run,
        revision: i64,
    ) -> CoreResult<()> {
        if run.status != RunStatus::Active {
            return Err(CoreError::Invalid("run is not active".into()));
        }
        if duration_expired(run) {
            self.stop_for_limit(ctx, run, revision, "run duration limit reached")?;
            return Err(CoreError::Recoverable(
                "run duration limit reached; run stopped".into(),
            ));
        }
        Ok(())
    }

    fn stop_for_limit(
        &self,
        ctx: &SessionContext,
        run: &Run,
        revision: i64,
        reason: &str,
    ) -> CoreResult<()> {
        let mut stopped = run.clone();
        stopped.status = RunStatus::Stopped;
        stopped.outcome = Some(RunOutcome {
            reason: reason.into(),
            remaining_task_ids: self
                .all_tasks(run.id)?
                .into_iter()
                .filter(|record| record.task.status != TaskStatus::Completed)
                .map(|record| record.task.id)
                .collect(),
        });
        let request = json!({"run_id":run.id,"reason":reason});
        let key = IdempotencyKey(format!("limit:{}", run.id.0));
        let changes = vec![StateChange::update(
            EntityKind::Run,
            run.id.0.to_string(),
            scope_for_run(&stopped),
            revision,
            &stopped,
        )
        .map_err(storage_error)?];
        let event = event_for(
            ctx,
            Some(run.project_id),
            Some(run.id),
            None,
            "run.limit_reached",
            json!({"reason":reason,"remaining_task_ids":stopped.outcome.as_ref().unwrap().remaining_task_ids}),
        );
        self.commit_run(
            ctx,
            "runs.limit_reached",
            &key,
            &request,
            &stopped,
            changes,
            event,
        )?;
        Ok(())
    }

    fn bump_run(&self, run: &mut Run, revision: i64) -> CoreResult<StateChange> {
        StateChange::update(
            EntityKind::Run,
            run.id.0.to_string(),
            scope_for_run(run),
            revision,
            run,
        )
        .map_err(storage_error)
    }

    fn task_update(
        &self,
        record: &TaskRecord,
        revision: i64,
        run: &Run,
    ) -> CoreResult<StateChange> {
        StateChange::update(
            EntityKind::Task,
            record.task.id.0.to_string(),
            scope_for_task(run, &record.task),
            revision,
            record,
        )
        .map_err(storage_error)
    }

    fn unblock_dependents(&self, run: &Run, completed: TaskId) -> CoreResult<Vec<StateChange>> {
        let mut changes = Vec::new();
        for mut candidate in self.all_tasks(run.id)? {
            if !candidate.waiting_on_dependencies || !candidate.dependencies.contains(&completed) {
                continue;
            }
            let mut all_complete = true;
            for id in &candidate.dependencies {
                if *id != completed && self.load_task(*id)?.0.task.status != TaskStatus::Completed {
                    all_complete = false;
                    break;
                }
            }
            if all_complete {
                candidate.waiting_on_dependencies = false;
                candidate.task.status = TaskStatus::Ready;
                let (_, revision) = self.load_task(candidate.task.id)?;
                changes.push(self.task_update(&candidate, revision, run)?);
            }
        }
        Ok(changes)
    }

    fn active_assignment_count(&self, run_id: RunId) -> CoreResult<u32> {
        Ok(self
            .all_tasks(run_id)?
            .iter()
            .filter(|record| {
                matches!(
                    record.task.status,
                    TaskStatus::Assigned | TaskStatus::Coding
                )
            })
            .count() as u32)
    }

    fn current_assignment(&self, task_id: TaskId) -> CoreResult<Option<(AssignmentRecord, i64)>> {
        let assignments = self.all_entities::<AssignmentRecord>(
            EntityKind::Assignment,
            EntityScope {
                task_id: Some(task_id.0.to_string()),
                ..Default::default()
            },
        )?;
        for assignment in assignments.into_iter().rev() {
            if assignment.status == AssignmentStatus::Active {
                if let Some(saved) = self
                    .db
                    .load::<AssignmentRecord>(EntityKind::Assignment, &assignment.id)
                    .map_err(storage_error)?
                {
                    return Ok(Some((saved.0, saved.1)));
                }
            }
        }
        Ok(None)
    }

    fn ensure_no_workspace_mutation(&self, assignment: &AssignmentRecord) -> CoreResult<()> {
        let scope = EntityScope {
            project_id: Some(assignment.project_id.0.to_string()),
            owner_id: Some(assignment.assignee.0.to_string()),
            ..Default::default()
        };
        let intents = self.all_entities::<GitOperationIntent>(EntityKind::GitOperation, scope)?;
        if intents.iter().any(|intent| {
            intent.assignment_key == assignment.id
                && matches!(
                    intent.checkpoint,
                    GitCheckpoint::MutationStarted | GitCheckpoint::GitApplied
                )
        }) {
            return Err(CoreError::Recoverable(
                "assignment has an in-flight Git workspace operation; retry after reconciliation"
                    .into(),
            ));
        }
        Ok(())
    }

    fn load_run(&self, run_id: RunId) -> CoreResult<(Run, i64)> {
        self.db
            .load(EntityKind::Run, &run_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn load_task(&self, task_id: TaskId) -> CoreResult<(TaskRecord, i64)> {
        self.db
            .load(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn load_task_run(&self, task_id: TaskId) -> CoreResult<(Run, i64)> {
        let (task, _) = self.load_task(task_id)?;
        self.load_run(task.task.run_id)
    }

    fn all_tasks(&self, run_id: RunId) -> CoreResult<Vec<TaskRecord>> {
        self.all_entities(
            EntityKind::Task,
            EntityScope {
                run_id: Some(run_id.0.to_string()),
                ..Default::default()
            },
        )
    }

    fn all_entities<T: for<'de> Deserialize<'de>>(
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

    fn prior_task<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
    ) -> CoreResult<Option<Mutation<Task>>> {
        self.db
            .prior_mutation(ctx, operation, key, request)
            .map_err(storage_error)
    }

    fn prior_run<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
    ) -> CoreResult<Option<Mutation<Run>>> {
        self.db
            .prior_mutation(ctx, operation, key, request)
            .map_err(storage_error)
    }

    fn commit_task<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        task: &Task,
        changes: Vec<StateChange>,
        event: NewEvent,
    ) -> CoreResult<Mutation<Task>> {
        self.commit_task_with_preconditions(
            ctx,
            operation,
            key,
            request,
            task,
            changes,
            event,
            Vec::new(),
            None,
        )
    }

    fn commit_task_with_preconditions<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        task: &Task,
        changes: Vec<StateChange>,
        event: NewEvent,
        preconditions: Vec<StatePrecondition>,
        lease_check: Option<(TaskId, ActorId)>,
    ) -> CoreResult<Mutation<Task>> {
        match self.db.commit_mutation(MutationInput {
            context: ctx,
            operation,
            idempotency_key: key,
            request,
            preconditions,
            result: task,
            changes,
            event,
        }) {
            Ok(result) => Ok(result),
            Err(PersistenceError::StateConflict(_)) => {
                if let Some(prior) = self.prior_task(ctx, operation, key, request)? {
                    return Ok(prior);
                }
                if let Some((task_id, owner_id)) = lease_check {
                    if self.leases.expired_lease_fence(task_id, owner_id).is_err() {
                        return Err(CoreError::Recoverable(
                            "lease renewed or became unavailable; refusing reassignment".into(),
                        ));
                    }
                    return Err(CoreError::Recoverable(
                        "lease is still expired but state changed concurrently; retry reassignment"
                            .into(),
                    ));
                }
                Err(CoreError::Recoverable(
                    "task or run changed concurrently; retry this operation".into(),
                ))
            }
            Err(error) => Err(storage_error(error)),
        }
    }

    fn commit_run<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        run: &Run,
        changes: Vec<StateChange>,
        event: NewEvent,
    ) -> CoreResult<Mutation<Run>> {
        match self.db.commit_mutation(MutationInput {
            context: ctx,
            operation,
            idempotency_key: key,
            request,
            preconditions: Vec::new(),
            result: run,
            changes,
            event,
        }) {
            Ok(result) => Ok(result),
            Err(PersistenceError::StateConflict(_)) => {
                if let Some(prior) = self.prior_run(ctx, operation, key, request)? {
                    return Ok(prior);
                }
                Err(CoreError::Recoverable(
                    "run changed concurrently; retry this operation".into(),
                ))
            }
            Err(error) => Err(storage_error(error)),
        }
    }
}

fn require_role(ctx: &SessionContext, role: Role) -> CoreResult<()> {
    if ctx.role() == role {
        Ok(())
    } else {
        Err(CoreError::Forbidden)
    }
}

fn validate_limits(limits: &WorkflowLimits) -> CoreResult<()> {
    if limits.max_duration_hours == 0
        || limits.max_concurrent_assignments == 0
        || limits.max_review_rounds == 0
    {
        return Err(CoreError::Invalid(
            "duration, concurrency, and review limits must be positive".into(),
        ));
    }
    if limits.max_duration_hours > i64::MAX as u64 / 3600 {
        return Err(CoreError::Invalid("max_duration_hours is too large".into()));
    }
    Ok(())
}

fn duration_expired(run: &Run) -> bool {
    let max = ChronoDuration::hours(run.limits.max_duration_hours.min(i64::MAX as u64) as i64);
    Utc::now().signed_duration_since(run.started_at) >= max
}

fn transition_allowed(from: TaskStatus, to: TaskStatus) -> bool {
    use TaskStatus::*;
    matches!(
        (from, to),
        (Ready, Assigned | Blocked | Failed | Cancelled)
            | (Assigned, Coding | Ready | Blocked | Failed | Cancelled)
            | (Coding, Submitted | Ready | Blocked | Failed | Cancelled)
            | (
                Submitted,
                InReview | RevisionRequired | Completed | Blocked | Failed | Cancelled
            )
            | (
                InReview,
                RevisionRequired | Completed | Blocked | Failed | Cancelled
            )
            | (
                RevisionRequired,
                InReview | Assigned | Blocked | Failed | Cancelled
            )
    )
}

fn event_for(
    ctx: &SessionContext,
    project_id: Option<ProjectId>,
    run_id: Option<RunId>,
    task_id: Option<TaskId>,
    event_type: &str,
    payload: serde_json::Value,
) -> NewEvent {
    NewEvent::from_context(
        ctx,
        ctx.actor_id().0.to_string(),
        EventScope {
            project_id,
            run_id,
            task_id,
        },
        event_type,
        payload,
    )
}

fn scope_for_run(run: &Run) -> EntityScope {
    EntityScope {
        project_id: Some(run.project_id.0.to_string()),
        ..Default::default()
    }
}
fn scope_for_task(run: &Run, task: &Task) -> EntityScope {
    EntityScope {
        project_id: Some(run.project_id.0.to_string()),
        run_id: Some(run.id.0.to_string()),
        task_id: Some(task.id.0.to_string()),
        owner_id: task.owner.map(|owner| owner.0.to_string()),
    }
}
fn assignment_scope(assignment: &AssignmentRecord) -> EntityScope {
    EntityScope {
        project_id: Some(assignment.project_id.0.to_string()),
        run_id: Some(assignment.run_id.0.to_string()),
        task_id: Some(assignment.task_id.0.to_string()),
        owner_id: Some(assignment.assignee.0.to_string()),
    }
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
