//! F09 application composition for the durable F01-F07 feature services.

use super::{
    human::{HumanCoordinator, ProjectHumanGatePolicy},
    reviews::{ProjectReviewPolicy, ReviewCoordinator, ReviewPolicy as CoordinationReviewPolicy},
    sessions::{AssignmentLeaseRecord, SessionManager},
    tasks::{ProjectWorkflowPolicy, TaskEngine},
    Coordination,
};
use crate::{
    artifacts::{ArtifactError, ExportedRunArtifacts, RunArtifactExporter},
    core::*,
    git::GitWorkspaceManager,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
    },
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashSet, path::Path, sync::Arc};
use uuid::Uuid;

const PAGE_SIZE: u32 = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProjectRegistrationRecord {
    pub id: ProjectId,
    pub name: String,
    pub config: ValidatedProjectConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RegisterProjectRequest {
    config_path: String,
    config: ValidatedProjectConfig,
}

#[derive(Debug, Deserialize)]
struct CredentialSummary {
    actor_id: Option<ActorId>,
    status: String,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordOmpEvent {
    run_id: RunId,
    event: OmpEvent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum OmpEvent {
    WorkerAttemptStarted {
        attempt_id: Uuid,
        worker: String,
        role: OmpWorkerRole,
        model: Option<String>,
    },
    WorkerAttemptFinished {
        attempt_id: Uuid,
        worker: String,
        outcome: OmpAttemptOutcome,
        exit_code: Option<i32>,
        failure_code: Option<OmpFailureCode>,
    },
    RunResumed {
        phase: OmpRunPhase,
        round: u32,
        previous_model: Option<String>,
        model: Option<String>,
    },
    NotificationDelivered {
        channel: OmpNotificationChannel,
        omp_session_id: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OmpWorkerRole {
    Planner,
    UxDesigner,
    PlanSelector,
    DesignReconciler,
    Coder,
    Reviewer,
    ComparisonReviewer,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OmpAttemptOutcome {
    Succeeded,
    InvalidOutput,
    ExitFailure,
    LaunchFailure,
    NoFinalResult,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OmpFailureCode {
    UsageLimitReached,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OmpRunPhase {
    Planning,
    Coding,
    Review,
    Revision,
    Integrating,
    Comparing,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OmpNotificationChannel {
    ToolResult,
    AgentMessage,
}

/// Concrete runtime composition. Authorization and lifecycle rules stay in their feature owners.
pub struct Application {
    db: Database,
    sessions: Arc<SessionManager>,
    git: Arc<GitWorkspaceManager>,
    human: Arc<HumanCoordinator>,
    reviews: Arc<ReviewCoordinator>,
    tasks: Arc<TaskEngine>,
}

impl Application {
    pub fn new(db: Database, workspace_root: impl AsRef<Path>) -> CoreResult<Self> {
        let workspace_root = workspace_root.as_ref().to_owned();
        let sessions = Arc::new(SessionManager::new(db.clone()));
        let git = Arc::new(GitWorkspaceManager::new(db.clone(), &workspace_root)?);
        let policies = Arc::new(ProjectPolicies { db: db.clone() });
        let human = Arc::new(HumanCoordinator::new(db.clone(), policies.clone()));
        let reviews = Arc::new(ReviewCoordinator::new(
            db.clone(),
            policies.clone(),
            git.clone(),
            human.clone(),
        ));
        let tasks = Arc::new(TaskEngine::new(
            db.clone(),
            policies.clone(),
            reviews.clone(),
            human.clone(),
            sessions.clone(),
        ));
        Ok(Self {
            db,
            sessions,
            git,
            human,
            reviews,
            tasks,
        })
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn sessions(&self) -> &SessionManager {
        &self.sessions
    }

    pub fn prepare_bootstrap(&self) -> CoreResult<String> {
        self.sessions.prepare_bootstrap()
    }

    pub fn bootstrap_supervisor(
        &self,
        bootstrap_secret: &str,
        actor_name: &str,
        key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        self.sessions
            .bootstrap_supervisor(bootstrap_secret, actor_name, key)
    }

    pub fn authenticate(&self, credential: &str) -> CoreResult<SessionContext> {
        self.sessions.authenticate(credential)
    }

    /// Local CLI only: possession of the old supervisor credential can renew a lapsed
    /// session without making expired credentials valid through ordinary authentication.
    pub fn recover_local_supervisor(&self, old_secret: &str) -> CoreResult<SessionContext> {
        self.sessions.recover_local_supervisor(
            old_secret,
            IdempotencyKey(Uuid::new_v4().to_string()),
        )
    }

    pub fn issue_agent(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        role: AgentRole,
        key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        self.sessions.issue_agent(supervisor, actor_name, role, key)
    }

    pub fn issue_cli(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        key: IdempotencyKey,
    ) -> CoreResult<IssuedCredential> {
        self.sessions.issue_cli(supervisor, actor_name, key)
    }

    pub fn revoke_session(
        &self,
        supervisor: &SessionContext,
        session: SessionId,
        key: IdempotencyKey,
    ) -> CoreResult<()> {
        self.sessions.revoke(supervisor, session, key)
    }

    pub fn register_project(
        &self,
        ctx: &SessionContext,
        config: ValidatedProjectConfig,
        config_path: impl AsRef<Path>,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Project>> {
        if ctx.role() != Role::Supervisor {
            return Err(CoreError::Forbidden);
        }
        let config_path = config_path
            .as_ref()
            .canonicalize()
            .map_err(|error| CoreError::Invalid(format!("cannot resolve config path: {error}")))?
            .to_string_lossy()
            .into_owned();
        let request = RegisterProjectRequest {
            config_path,
            config: config.clone(),
        };
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, "projects.register", &key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        let existing: Page<Project> = self
            .db
            .list_entities(EntityKind::Project, &EntityScope::default(), first_page())
            .map_err(storage_error)?;
        if existing
            .items
            .iter()
            .any(|project| project.name == config.name)
        {
            return Err(CoreError::Invalid(format!(
                "project {:?} is already registered",
                config.name
            )));
        }
        let project = Project {
            id: ProjectId::new(),
            name: config.name.clone(),
        };
        let record = ProjectRegistrationRecord {
            id: project.id,
            name: project.name.clone(),
            config: config.clone(),
        };
        let mut changes = vec![StateChange::insert(
            EntityKind::Project,
            project.id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                ..Default::default()
            },
            &record,
        )
        .map_err(storage_error)?];
        let mut repository_ids = Vec::with_capacity(config.repositories.len());
        for configured in &config.repositories {
            let repository = Repository {
                id: RepositoryId::new(),
                project_id: project.id,
                name: configured.id.clone(),
                canonical_path: configured.canonical_path.clone(),
            };
            repository_ids.push(repository.id);
            changes.push(
                StateChange::insert(
                    EntityKind::Repository,
                    repository.id.0.to_string(),
                    EntityScope {
                        project_id: Some(project.id.0.to_string()),
                        ..Default::default()
                    },
                    &repository,
                )
                .map_err(storage_error)?,
            );
        }
        let event = NewEvent::from_context(
            ctx,
            "supervisor",
            EventScope {
                project_id: Some(project.id),
                run_id: None,
                task_id: None,
            },
            "project.registered",
            json!({"project_id":project.id,"repository_ids":repository_ids}),
        );
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: "projects.register",
                idempotency_key: &key,
                request: &request,
                preconditions: vec![],
                result: &project,
                changes,
                event,
            })
            .map_err(storage_error)
    }

    pub fn export_run(
        &self,
        ctx: &SessionContext,
        run_id: RunId,
    ) -> CoreResult<ExportedRunArtifacts> {
        let run = self.load_run(run_id)?;
        if !can_read_project(ctx, run.project_id) {
            return Err(CoreError::Forbidden);
        }
        let config = ProjectPolicies {
            db: self.db.clone(),
        }
        .project_config(run.project_id)?;
        RunArtifactExporter::new(self.db.clone(), config.artifact_dir)
            .export_run(run_id)
            .map_err(artifact_error)
    }

    pub fn inspect_workspace(
        &self,
        ctx: &SessionContext,
        request: InspectWorkspace,
    ) -> CoreResult<WorkspaceInspection> {
        self.git.inspect_workspace(ctx, request)
    }

    pub fn cleanup_workspace(
        &self,
        ctx: &SessionContext,
        request: CleanupWorkspace,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        self.git.cleanup_workspace(ctx, request, key)
    }

    /// Records a bridge observation, without changing coordinator run state.
    pub(crate) fn record_omp_event(
        &self,
        ctx: &SessionContext,
        request: RecordOmpEvent,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<()>> {
        if ctx.role() != Role::Supervisor {
            return Err(CoreError::Forbidden);
        }
        let run = self.load_run(request.run_id)?;
        let (event_type, payload) = match &request.event {
            OmpEvent::WorkerAttemptStarted {
                attempt_id,
                worker,
                role,
                model,
            } => (
                "worker.attempt_started",
                json!({"attempt_id":attempt_id,"worker":worker,"role":role,"model":model}),
            ),
            OmpEvent::WorkerAttemptFinished {
                attempt_id,
                worker,
                outcome,
                exit_code,
                failure_code,
            } => (
                "worker.attempt_finished",
                json!({"attempt_id":attempt_id,"worker":worker,"outcome":outcome,"exit_code":exit_code,"failure_code":failure_code}),
            ),
            OmpEvent::RunResumed {
                phase,
                round,
                previous_model,
                model,
            } => (
                "run.resumed",
                json!({"phase":phase,"round":round,"previous_model":previous_model,"model":model}),
            ),
            OmpEvent::NotificationDelivered {
                channel,
                omp_session_id,
            } => (
                "notification.delivered",
                json!({"kind":"completion","target":"driving_omp","channel":channel,"omp_session_id":omp_session_id}),
            ),
        };
        let event = NewEvent::from_context(
            ctx,
            "supervisor",
            EventScope {
                project_id: Some(run.project_id),
                run_id: Some(run.id),
                task_id: None,
            },
            event_type,
            payload,
        );
        self.db
            .record_observation(ctx, "omp.events.record", &key, &request, event)
            .map_err(storage_error)
    }

    fn load_run(&self, run_id: RunId) -> CoreResult<Run> {
        self.db
            .load::<Run>(EntityKind::Run, &run_id.0.to_string())
            .map_err(storage_error)?
            .map(|(run, _)| run)
            .ok_or(CoreError::NotFound)
    }

    fn all_entities<T: for<'de> Deserialize<'de>>(
        &self,
        kind: EntityKind,
        scope: EntityScope,
    ) -> CoreResult<Vec<T>> {
        let mut items = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .db
                .list_entities::<T>(
                    kind,
                    &scope,
                    PageRequest {
                        limit: PAGE_SIZE,
                        after,
                    },
                )
                .map_err(storage_error)?;
            let len = page.items.len();
            items.extend(page.items);
            if len < PAGE_SIZE as usize {
                break;
            }
            after = page.next_cursor.map(EventCursor);
        }
        Ok(items)
    }
}

impl Coordination for Application {
    fn list_projects(&self, _ctx: &SessionContext, page: PageRequest) -> CoreResult<Page<Project>> {
        self.db
            .list_entities(EntityKind::Project, &EntityScope::default(), page)
            .map_err(storage_error)
    }

    fn renew_session(
        &self,
        ctx: &SessionContext,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<DateTime<Utc>>> {
        self.sessions.renew_session(ctx, key)
    }

    fn renew_assignment_lease(
        &self,
        ctx: &SessionContext,
        task_id: TaskId,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<AssignmentLeaseRecord>> {
        self.sessions.renew_assignment_lease(ctx, task_id, key)
    }

    fn list_repositories(
        &self,
        _ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Repository>> {
        self.db
            .list_entities(
                EntityKind::Repository,
                &EntityScope {
                    project_id: Some(project.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(storage_error)
    }

    fn list_agents(
        &self,
        _ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Agent>> {
        self.db
            .load::<Project>(EntityKind::Project, &project.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let actors: Page<Actor> = self
            .db
            .list_entities(EntityKind::Actor, &EntityScope::default(), page)
            .map_err(storage_error)?;
        let active: HashSet<ActorId> = self
            .all_entities::<CredentialSummary>(EntityKind::Credential, EntityScope::default())?
            .into_iter()
            .filter(|credential| {
                credential.status == "active"
                    && credential
                        .expires_at
                        .is_some_and(|expiry| expiry > Utc::now())
            })
            .filter_map(|credential| credential.actor_id)
            .collect();
        Ok(Page {
            items: actors
                .items
                .into_iter()
                .map(|actor| Agent {
                    actor_id: actor.id,
                    display_name: actor.display_name,
                    role: actor.role,
                    status: if active.contains(&actor.id) {
                        AgentStatus::Active
                    } else {
                        AgentStatus::Disconnected
                    },
                })
                .collect(),
            next_cursor: actors.next_cursor,
        })
    }

    fn list_runs(
        &self,
        _ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Run>> {
        self.db
            .list_entities(
                EntityKind::Run,
                &EntityScope {
                    project_id: Some(project.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(storage_error)
    }

    fn list_tasks(
        &self,
        ctx: &SessionContext,
        run: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>> {
        self.tasks.list_tasks(ctx, run, page)
    }

    fn available_work(
        &self,
        ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>> {
        self.tasks.available_work(ctx, project, page)
    }

    fn create_run(
        &self,
        ctx: &SessionContext,
        request: CreateRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        self.tasks.create_run(ctx, request, key)
    }

    fn create_comparison_run(
        &self,
        ctx: &SessionContext,
        request: CreateComparisonRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        self.tasks.create_comparison_run(ctx, request, key)
    }

    fn select_candidate(
        &self,
        ctx: &SessionContext,
        request: SelectCandidate,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        self.tasks.select_candidate(ctx, request, key)
    }

    fn repair_task(
        &self,
        ctx: &SessionContext,
        request: RepairTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<RepairResult>> {
        self.tasks.repair_task(ctx, request, key)
    }

    fn create_task(
        &self,
        ctx: &SessionContext,
        request: CreateTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.create_task(ctx, request, key)
    }

    fn create_task_with_dependencies(
        &self,
        ctx: &SessionContext,
        request: CreateTaskWithDependencies,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.create_task_with_dependencies(ctx, request, key)
    }

    fn assign_task(
        &self,
        ctx: &SessionContext,
        request: AssignTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.assign_task(ctx, request, key)
    }

    fn claim_task(
        &self,
        ctx: &SessionContext,
        request: ClaimTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.claim_task(ctx, request, key)
    }

    fn release_task(
        &self,
        ctx: &SessionContext,
        request: ReleaseTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.release_task(ctx, request, key)
    }

    fn update_progress(
        &self,
        ctx: &SessionContext,
        request: UpdateProgress,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.update_progress(ctx, request, key)
    }

    fn submit_task(
        &self,
        ctx: &SessionContext,
        request: SubmitTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.submit_task(ctx, request, key)
    }

    fn record_task_disposition(
        &self,
        ctx: &SessionContext,
        request: FinalTaskDisposition,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.tasks.record_task_disposition(ctx, request, key)
    }

    fn complete_run(
        &self,
        ctx: &SessionContext,
        request: CompleteRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        self.tasks.complete_run(ctx, request, key)
    }

    fn send_message(
        &self,
        ctx: &SessionContext,
        request: SendMessage,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Message>> {
        self.sessions.send_message(ctx, request, key)
    }

    fn read_messages(
        &self,
        ctx: &SessionContext,
        request: ReadMessages,
    ) -> CoreResult<Page<Message>> {
        self.sessions.read_messages(ctx, request)
    }

    fn request_workspace(
        &self,
        ctx: &SessionContext,
        request: WorkspaceRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        self.git.request_workspace(ctx, request, key)
    }

    fn request_review(
        &self,
        ctx: &SessionContext,
        request: ReviewRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<ReviewRound>> {
        self.reviews.request_review(ctx, request, key, &self.tasks)
    }

    fn submit_review(
        &self,
        ctx: &SessionContext,
        request: SubmitReview,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Review>> {
        self.reviews.submit_review(ctx, request, key)
    }

    fn list_reviews(&self, ctx: &SessionContext, request: ListReviews) -> CoreResult<Page<Review>> {
        self.reviews.list_reviews(ctx, request)
    }

    fn list_review_rounds(
        &self,
        ctx: &SessionContext,
        request: ListReviewRounds,
    ) -> CoreResult<Page<ReviewRound>> {
        self.reviews.list_review_rounds(ctx, request)
    }

    fn disposition_findings(
        &self,
        ctx: &SessionContext,
        request: DispositionFindings,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>> {
        self.reviews
            .disposition_findings(ctx, request, key, &self.tasks)
    }

    fn request_human(
        &self,
        ctx: &SessionContext,
        request: HumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>> {
        self.human.request_human(ctx, request, key)
    }

    fn list_human_requests(
        &self,
        ctx: &SessionContext,
        request: ListHumanRequests,
    ) -> CoreResult<Page<HumanDecision>> {
        self.human.list_human_requests(ctx, request)
    }

    fn resolve_human(
        &self,
        ctx: &SessionContext,
        request: ResolveHumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>> {
        self.human.resolve_human(ctx, request, key)
    }

    fn inspect_workspace(
        &self,
        ctx: &SessionContext,
        request: InspectWorkspace,
    ) -> CoreResult<WorkspaceInspection> {
        self.git.inspect_workspace(ctx, request)
    }

    fn capture_commit_refs(
        &self,
        ctx: &SessionContext,
        request: CaptureWorkspaceCommits,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Vec<CommitRef>>> {
        self.git.capture_commit_refs(ctx, request, key)
    }

    fn cleanup_workspace(
        &self,
        ctx: &SessionContext,
        request: CleanupWorkspace,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        self.git.cleanup_workspace(ctx, request, key)
    }

    fn timeline(
        &self,
        _ctx: &SessionContext,
        scope: EventScope,
        page: PageRequest,
    ) -> CoreResult<Page<Event>> {
        self.db
            .events_after(scope, page.after.as_ref(), page.limit)
            .map_err(storage_error)
    }

    fn locate_artifacts(
        &self,
        ctx: &SessionContext,
        run_id: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<ArtifactRef>> {
        if !can_read_project(ctx, self.load_run(run_id)?.project_id) {
            return Err(CoreError::Forbidden);
        }
        if page.limit == 0 || page.limit > PAGE_SIZE {
            return Err(CoreError::Invalid(format!(
                "page limit must be between 1 and {PAGE_SIZE}"
            )));
        }
        let run = self.load_run(run_id)?;
        let config = ProjectPolicies {
            db: self.db.clone(),
        }
        .project_config(run.project_id)?;
        let all = RunArtifactExporter::new(self.db.clone(), config.artifact_dir)
            .locate_run(run_id)
            .map_err(artifact_error)?;
        let offset = match page.after {
            None => 0,
            Some(EventCursor(cursor)) => parse_artifact_cursor(&cursor, run_id)?,
        };
        if offset > all.len() {
            return Err(CoreError::Invalid("artifact cursor is out of range".into()));
        }
        let end = (offset + page.limit as usize).min(all.len());
        Ok(Page {
            items: all[offset..end].to_vec(),
            next_cursor: (end > offset).then(|| format!("artifacts:v1:{}:{end}", run_id.0)),
        })
    }
}

impl SessionAuthenticator for Application {
    fn authenticate(&self, bearer_credential: &str) -> CoreResult<SessionContext> {
        Application::authenticate(self, bearer_credential)
    }
}

struct ProjectPolicies {
    db: Database,
}

impl ProjectPolicies {
    fn project_config(&self, project_id: ProjectId) -> CoreResult<ValidatedProjectConfig> {
        self.db
            .load::<ProjectRegistrationRecord>(EntityKind::Project, &project_id.0.to_string())
            .map_err(storage_error)?
            .map(|(record, _)| record.config)
            .ok_or(CoreError::NotFound)
    }
}

impl ProjectWorkflowPolicy for ProjectPolicies {
    fn workflow_limits(&self, project_id: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(self.project_config(project_id)?.workflow_limits)
    }

    fn comparison_policies(&self, project_id: ProjectId) -> CoreResult<(WorkflowLimits, u32)> {
        let config = self.project_config(project_id)?;
        Ok((
            config.workflow_limits,
            config.review_policy.min_independent_reviewers,
        ))
    }
}

impl ProjectReviewPolicy for ProjectPolicies {
    fn review_policy(&self, project_id: ProjectId) -> CoreResult<CoordinationReviewPolicy> {
        let config = self.project_config(project_id)?;
        Ok(CoordinationReviewPolicy {
            min_independent_reviewers: config.review_policy.min_independent_reviewers,
            high_severity_defer_requires_human: config
                .human_gates
                .is_some_and(|gate| gate.high_severity_deferral_approval),
        })
    }
}

impl ProjectHumanGatePolicy for ProjectPolicies {
    fn human_gate_policy(&self, project_id: ProjectId) -> CoreResult<Option<HumanGatePolicy>> {
        Ok(self.project_config(project_id)?.human_gates)
    }
}

fn first_page() -> PageRequest {
    PageRequest {
        limit: PAGE_SIZE,
        after: None,
    }
}

fn can_read_project(ctx: &SessionContext, _project_id: ProjectId) -> bool {
    matches!(
        ctx.role(),
        Role::Supervisor | Role::Coder | Role::Reviewer | Role::Cli
    )
}

fn parse_artifact_cursor(cursor: &str, run_id: RunId) -> CoreResult<usize> {
    let prefix = format!("artifacts:v1:{}:", run_id.0);
    cursor
        .strip_prefix(&prefix)
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| CoreError::Invalid("artifact cursor does not match this run".into()))
}

fn storage_error(error: PersistenceError) -> CoreError {
    match error {
        PersistenceError::IdempotencyConflict => CoreError::IdempotencyConflict,
        PersistenceError::StateConflict(_) => {
            CoreError::Recoverable("state changed concurrently; retry the operation".into())
        }
        other => CoreError::Internal(other.to_string()),
    }
}

fn artifact_error(error: ArtifactError) -> CoreError {
    CoreError::Internal(error.to_string())
}
