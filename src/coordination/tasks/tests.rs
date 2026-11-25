use super::*;
use crate::core::{Actor, SessionId};
use std::{fs, path::PathBuf};

struct TempDb(PathBuf);
impl TempDb {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("tesselate-f04-{}", Uuid::new_v4()));
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
struct Policy;
impl ProjectWorkflowPolicy for Policy {
    fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 2,
            max_task_retries: 2,
            max_review_rounds: 3,
        })
    }

    fn comparison_policies(&self, project_id: ProjectId) -> CoreResult<(WorkflowLimits, u32)> {
        Ok((self.workflow_limits(project_id)?, 2))
    }
}
struct Reviews;
impl ReviewCompletionGate for Reviews {
    fn ensure_task_complete(&self, _: TaskId) -> CoreResult<Vec<StatePrecondition>> {
        Ok(Vec::new())
    }
    fn reviewed_commits_for_run(&self, _: RunId) -> CoreResult<Vec<CommitRef>> {
        Ok(Vec::new())
    }
}
#[derive(Default)]
struct Human(std::sync::Mutex<Vec<TransitionContext>>);
impl HumanTransitionGate for Human {
    fn ensure_allowed(&self, _: &SessionContext, transition: TransitionContext) -> CoreResult<()> {
        self.0.lock().unwrap().push(transition);
        Ok(())
    }
}
struct Leases;
impl AssignmentLeaseStatus for Leases {
    fn expired_lease_fence(&self, _: TaskId, _: ActorId) -> CoreResult<LeaseFence> {
        Err(CoreError::Recoverable("lease is active".into()))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseState {
    task_id: TaskId,
    owner_id: ActorId,
    expired: bool,
}
struct DatabaseLeases {
    db: Database,
    entity_id: String,
}
impl AssignmentLeaseStatus for DatabaseLeases {
    fn expired_lease_fence(&self, task_id: TaskId, owner: ActorId) -> CoreResult<LeaseFence> {
        let (lease, revision) = self
            .db
            .load::<LeaseState>(EntityKind::Session, &self.entity_id)
            .map_err(|error| CoreError::Internal(error.to_string()))?
            .ok_or(CoreError::NotFound)?;
        if lease.task_id != task_id || lease.owner_id != owner || !lease.expired {
            return Err(CoreError::Recoverable(
                "lease is active or no longer belongs to this task".into(),
            ));
        }
        Ok(LeaseFence {
            task_id,
            owner_id: owner,
            condition: StatePrecondition {
                kind: EntityKind::Session,
                id: self.entity_id.clone(),
                expected_revision: revision,
            },
        })
    }
}
struct RenewOnAssignment {
    db: Database,
    lease_id: String,
    project_id: ProjectId,
    run_id: RunId,
    task_id: TaskId,
}
impl HumanTransitionGate for RenewOnAssignment {
    fn ensure_allowed(
        &self,
        ctx: &SessionContext,
        transition: TransitionContext,
    ) -> CoreResult<()> {
        if transition.identity == TransitionIdentity::TaskAssignment
            && matches!(
                transition.from,
                Some(LifecycleState::Task(
                    TaskStatus::Assigned | TaskStatus::Coding
                ))
            )
        {
            let (mut lease, revision) = self
                .db
                .load::<LeaseState>(EntityKind::Session, &self.lease_id)
                .map_err(|error| CoreError::Internal(error.to_string()))?
                .ok_or(CoreError::NotFound)?;
            lease.expired = false;
            let change = StateChange::update(
                EntityKind::Session,
                self.lease_id.clone(),
                EntityScope::default(),
                revision,
                &lease,
            )
            .map_err(|error| CoreError::Internal(error.to_string()))?;
            self.db
                .commit_mutation(MutationInput {
                    context: ctx,
                    operation: "test.lease_renew",
                    idempotency_key: &IdempotencyKey("renew-during-reassign".into()),
                    request: &lease,
                    preconditions: Vec::new(),
                    result: &lease,
                    changes: vec![change],
                    event: NewEvent::from_context(
                        ctx,
                        "supervisor",
                        EventScope {
                            project_id: Some(self.project_id),
                            run_id: Some(self.run_id),
                            task_id: Some(self.task_id),
                        },
                        "lease.renewed",
                        serde_json::json!({}),
                    ),
                })
                .map_err(|error| CoreError::Internal(error.to_string()))?;
        }
        Ok(())
    }
}
fn context(role: Role) -> SessionContext {
    SessionContext::validated(SessionId::new(), ActorId::new(), role)
}
fn engine(db: Database) -> TaskEngine {
    engine_with_human(db, Arc::new(Human::default()))
}
fn engine_with_human(db: Database, human: Arc<dyn HumanTransitionGate>) -> TaskEngine {
    TaskEngine::new(
        db,
        Arc::new(Policy),
        Arc::new(Reviews),
        human,
        Arc::new(Leases),
    )
}
fn transition_for_review(
    engine: &TaskEngine,
    ctx: &SessionContext,
    request: ReviewTaskTransition,
    key: IdempotencyKey,
) -> CoreResult<Mutation<Task>> {
    let (task, changes) =
        engine.prepare_review_transition(ctx, request.task_id, request.status, &request.reason)?;
    let run = engine.load_run(task.run_id)?.0;
    let event = event_for(
        ctx,
        Some(run.project_id),
        Some(run.id),
        Some(task.id),
        "task.review_status",
        serde_json::json!({"status":request.status,"reason":request.reason}),
    );
    engine.commit_task(
        ctx,
        "tasks.review_transition",
        &key,
        &request,
        &task,
        changes,
        event,
    )
}
fn seed_project(db: &Database, ctx: &SessionContext) -> ProjectId {
    let project = Project {
        id: ProjectId::new(),
        name: "test".into(),
    };
    let change = StateChange::insert(
        EntityKind::Project,
        project.id.0.to_string(),
        EntityScope::default(),
        &project,
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.project",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: &project,
        preconditions: Vec::new(),
        result: &project,
        changes: vec![change],
        event: NewEvent::from_context(
            ctx,
            "test",
            EventScope {
                project_id: Some(project.id),
                run_id: None,
                task_id: None,
            },
            "project.created",
            serde_json::json!({}),
        ),
    })
    .unwrap();
    project.id
}

fn seed_coder(db: &Database, ctx: &SessionContext) -> Actor {
    let coder = Actor {
        id: ActorId::new(),
        display_name: "coder".into(),
        role: Role::Coder,
    };
    let change = StateChange::insert(
        EntityKind::Actor,
        coder.id.0.to_string(),
        EntityScope::default(),
        &coder,
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.actor",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: &coder,
        preconditions: Vec::new(),
        result: &coder,
        changes: vec![change],
        event: NewEvent::from_context(
            ctx,
            "supervisor",
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None,
            },
            "actor.created",
            serde_json::json!({}),
        ),
    })
    .unwrap();
    coder
}

#[test]
fn transition_graph_rejects_skipping_submission_and_terminal_reopens() {
    assert!(transition_allowed(
        TaskStatus::Coding,
        TaskStatus::Submitted
    ));
    assert!(!transition_allowed(
        TaskStatus::Coding,
        TaskStatus::Completed
    ));
    assert!(!transition_allowed(
        TaskStatus::Completed,
        TaskStatus::Ready
    ));
}

#[test]
fn create_run_replays_and_dependency_unblocks_atomically() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let ctx = context(Role::Supervisor);
    let project = seed_project(&db, &ctx);
    let engine = engine(db.clone());
    let run_request = CreateRun {
        project_id: project,
        goal: "ship feature".into(),
    };
    let run_key = IdempotencyKey("create-run".into());
    let first = engine
        .create_run(&ctx, run_request.clone(), run_key.clone())
        .unwrap();
    let replay = engine.create_run(&ctx, run_request, run_key).unwrap();
    assert!(!first.replayed && replay.replayed);
    assert_eq!(first.event_id, replay.event_id);

    let prerequisite = engine
        .create_task(
            &ctx,
            CreateTask {
                run_id: first.value.id,
                title: "first".into(),
                goal: "do first".into(),
            },
            IdempotencyKey("task-one".into()),
        )
        .unwrap()
        .value;
    let dependent = engine
        .create_task_with_dependencies(
            &ctx,
            CreateTaskWithDependencies {
                run_id: first.value.id,
                title: "second".into(),
                goal: "do second".into(),
                dependencies: vec![prerequisite.id],
            },
            IdempotencyKey("task-two".into()),
        )
        .unwrap()
        .value;
    assert_eq!(dependent.status, TaskStatus::Blocked);

    let coder = seed_coder(&db, &ctx);
    let coder_ctx = SessionContext::validated(SessionId::new(), coder.id, Role::Coder);
    engine
        .assign_task(
            &ctx,
            AssignTask {
                task_id: prerequisite.id,
                assignee: coder.id,
            },
            IdempotencyKey("assign".into()),
        )
        .unwrap();
    engine
        .claim_task(
            &coder_ctx,
            ClaimTask {
                task_id: prerequisite.id,
            },
            IdempotencyKey("claim".into()),
        )
        .unwrap();
    engine
        .submit_task(
            &coder_ctx,
            SubmitTask {
                task_id: prerequisite.id,
                commits: vec![CommitRef {
                    repository_id: RepositoryId::new(),
                    commit_id: "abc123".into(),
                }],
                summary: "done".into(),
            },
            IdempotencyKey("submit".into()),
        )
        .unwrap();
    transition_for_review(
        &engine,
        &ctx,
        ReviewTaskTransition {
            task_id: prerequisite.id,
            status: TaskStatus::InReview,
            reason: "review requested".into(),
        },
        IdempotencyKey("review-state".into()),
    )
    .unwrap();
    transition_for_review(
        &engine,
        &ctx,
        ReviewTaskTransition {
            task_id: prerequisite.id,
            status: TaskStatus::RevisionRequired,
            reason: "revise".into(),
        },
        IdempotencyKey("revision-required".into()),
    )
    .unwrap();
    engine
        .assign_task(
            &ctx,
            AssignTask {
                task_id: prerequisite.id,
                assignee: coder.id,
            },
            IdempotencyKey("revision-assigned".into()),
        )
        .unwrap();
    engine
        .claim_task(
            &coder_ctx,
            ClaimTask {
                task_id: prerequisite.id,
            },
            IdempotencyKey("revision-claim".into()),
        )
        .unwrap();
    engine
        .submit_task(
            &coder_ctx,
            SubmitTask {
                task_id: prerequisite.id,
                commits: vec![CommitRef {
                    repository_id: RepositoryId::new(),
                    commit_id: "abc124".into(),
                }],
                summary: "revised".into(),
            },
            IdempotencyKey("revision-submit".into()),
        )
        .unwrap();
    transition_for_review(
        &engine,
        &ctx,
        ReviewTaskTransition {
            task_id: prerequisite.id,
            status: TaskStatus::InReview,
            reason: "ready for review".into(),
        },
        IdempotencyKey("review-state-2".into()),
    )
    .unwrap();

    engine
        .record_task_disposition(
            &ctx,
            FinalTaskDisposition {
                task_id: prerequisite.id,
                outcome: FinalTaskOutcome::Completed,
                reason: "done".into(),
            },
            IdempotencyKey("finish-one".into()),
        )
        .unwrap();
    let unlocked = db
        .load::<TaskRecord>(EntityKind::Task, &dependent.id.0.to_string())
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(unlocked.task.status, TaskStatus::Ready);
    assert!(!unlocked.waiting_on_dependencies);
    drop(db);
    let reopened = Database::open(&temp.0).unwrap();
    assert_eq!(
        reopened
            .load::<TaskRecord>(EntityKind::Task, &dependent.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .status,
        TaskStatus::Ready
    );
}

#[test]
fn concurrency_and_retry_limits_are_enforced() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let project = seed_project(&db, &supervisor);
    let engine = engine(db.clone());
    let run = engine
        .create_run(
            &supervisor,
            CreateRun {
                project_id: project,
                goal: "work".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    let coder = seed_coder(&db, &supervisor);
    let create = |name: &str| {
        engine
            .create_task(
                &supervisor,
                CreateTask {
                    run_id: run.id,
                    title: name.into(),
                    goal: name.into(),
                },
                IdempotencyKey(format!("task-{name}")),
            )
            .unwrap()
            .value
    };
    let first = create("first");
    let second = create("second");
    let third = create("third");
    for task in [first.id, second.id] {
        engine
            .assign_task(
                &supervisor,
                AssignTask {
                    task_id: task,
                    assignee: coder.id,
                },
                IdempotencyKey(format!("assign-{task:?}")),
            )
            .unwrap();
    }
    assert!(matches!(
        engine.assign_task(
            &supervisor,
            AssignTask {
                task_id: third.id,
                assignee: coder.id
            },
            IdempotencyKey("assign-third".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    let fourth = create("fourth");
    engine
        .release_task(
            &supervisor,
            ReleaseTask {
                task_id: first.id,
                reason: "retry".into(),
            },
            IdempotencyKey("release-1".into()),
        )
        .unwrap();
    for attempt in 0..2 {
        engine
            .assign_task(
                &supervisor,
                AssignTask {
                    task_id: first.id,
                    assignee: coder.id,
                },
                IdempotencyKey(format!("retry-assign-{attempt}")),
            )
            .unwrap();
        engine
            .release_task(
                &supervisor,
                ReleaseTask {
                    task_id: first.id,
                    reason: "retry".into(),
                },
                IdempotencyKey(format!("retry-release-{attempt}")),
            )
            .unwrap();
    }
    // Initial assignment plus two retries are allowed. The next assignment stops the run durably.
    assert!(matches!(
        engine.assign_task(
            &supervisor,
            AssignTask {
                task_id: first.id,
                assignee: coder.id
            },
            IdempotencyKey("retry-exhausted".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    let stopped = db
        .load::<Run>(EntityKind::Run, &run.id.0.to_string())
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(stopped.status, RunStatus::Stopped);
    assert!(stopped
        .outcome
        .unwrap()
        .remaining_task_ids
        .contains(&fourth.id));
}

#[test]
fn renewal_between_expiry_read_and_reassignment_commit_is_fenced() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let project = seed_project(&db, &supervisor);
    let base_engine = engine(db.clone());
    let run = base_engine
        .create_run(
            &supervisor,
            CreateRun {
                project_id: project,
                goal: "lease race".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    let first_coder = seed_coder(&db, &supervisor);
    let second_coder = seed_coder(&db, &supervisor);
    let task = base_engine
        .create_task(
            &supervisor,
            CreateTask {
                run_id: run.id,
                title: "task".into(),
                goal: "keep current owner".into(),
            },
            IdempotencyKey("task".into()),
        )
        .unwrap()
        .value;
    base_engine
        .assign_task(
            &supervisor,
            AssignTask {
                task_id: task.id,
                assignee: first_coder.id,
            },
            IdempotencyKey("assign-first".into()),
        )
        .unwrap();

    let lease_id = SessionId::new().0.to_string();
    let lease = LeaseState {
        task_id: task.id,
        owner_id: first_coder.id,
        expired: true,
    };
    let lease_change = StateChange::insert(
        EntityKind::Session,
        lease_id.clone(),
        EntityScope::default(),
        &lease,
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: &supervisor,
        operation: "test.lease",
        idempotency_key: &IdempotencyKey("lease".into()),
        request: &lease,
        preconditions: Vec::new(),
        result: &lease,
        changes: vec![lease_change],
        event: NewEvent::from_context(
            &supervisor,
            "supervisor",
            EventScope {
                project_id: Some(project),
                run_id: Some(run.id),
                task_id: Some(task.id),
            },
            "lease.expired",
            serde_json::json!({}),
        ),
    })
    .unwrap();

    let racing_engine = TaskEngine::new(
        db.clone(),
        Arc::new(Policy),
        Arc::new(Reviews),
        Arc::new(RenewOnAssignment {
            db: db.clone(),
            lease_id: lease_id.clone(),
            project_id: project,
            run_id: run.id,
            task_id: task.id,
        }),
        Arc::new(DatabaseLeases {
            db: db.clone(),
            entity_id: lease_id.clone(),
        }),
    );
    let result = racing_engine.assign_task(
        &supervisor,
        AssignTask {
            task_id: task.id,
            assignee: second_coder.id,
        },
        IdempotencyKey("reassign".into()),
    );
    assert!(matches!(result, Err(CoreError::Recoverable(_))));
    let current = db
        .load::<TaskRecord>(EntityKind::Task, &task.id.0.to_string())
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(current.task.status, TaskStatus::Assigned);
    assert_eq!(current.task.owner, Some(first_coder.id));
    assert!(
        !db.load::<LeaseState>(EntityKind::Session, &lease_id)
            .unwrap()
            .unwrap()
            .0
            .expired
    );
}

#[test]
fn available_work_allows_supervisors_and_persists_expired_run_stop() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let project = seed_project(&db, &supervisor);
    let engine = engine(db.clone());
    let run = engine
        .create_run(
            &supervisor,
            CreateRun {
                project_id: project,
                goal: "expires".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    let task = engine
        .create_task(
            &supervisor,
            CreateTask {
                run_id: run.id,
                title: "ready".into(),
                goal: "ready".into(),
            },
            IdempotencyKey("task".into()),
        )
        .unwrap()
        .value;
    assert_eq!(
        engine
            .available_work(
                &supervisor,
                project,
                PageRequest {
                    limit: 50,
                    after: None
                }
            )
            .unwrap()
            .items,
        vec![task.clone()]
    );

    let (mut expired, revision) = db
        .load::<Run>(EntityKind::Run, &run.id.0.to_string())
        .unwrap()
        .unwrap();
    expired.started_at = Utc::now() - ChronoDuration::hours(25);
    let change = StateChange::update(
        EntityKind::Run,
        run.id.0.to_string(),
        scope_for_run(&expired),
        revision,
        &expired,
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: &supervisor,
        operation: "test.expire_run",
        idempotency_key: &IdempotencyKey("expire".into()),
        request: &expired,
        preconditions: Vec::new(),
        result: &expired,
        changes: vec![change],
        event: NewEvent::from_context(
            &supervisor,
            "supervisor",
            EventScope {
                project_id: Some(project),
                run_id: Some(run.id),
                task_id: None,
            },
            "run.aged",
            serde_json::json!({}),
        ),
    })
    .unwrap();
    let work = engine
        .available_work(
            &supervisor,
            project,
            PageRequest {
                limit: 50,
                after: None,
            },
        )
        .unwrap();
    assert!(work.items.is_empty());
    let events_after_stop = db.event_count().unwrap();
    let repeated_poll = engine
        .available_work(
            &supervisor,
            project,
            PageRequest {
                limit: 50,
                after: None,
            },
        )
        .unwrap();
    assert!(repeated_poll.items.is_empty());
    assert_eq!(db.event_count().unwrap(), events_after_stop);
    let stopped = db
        .load::<Run>(EntityKind::Run, &run.id.0.to_string())
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(stopped.status, RunStatus::Stopped);
    let outcome = stopped.outcome.unwrap();
    assert!(outcome.reason.contains("duration"));
    assert_eq!(outcome.remaining_task_ids, vec![task.id]);
}

#[test]
fn human_gate_receives_specific_run_completion_context() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let project = seed_project(&db, &supervisor);
    let human = Arc::new(Human::default());
    let engine = engine_with_human(db, human.clone());
    let run = engine
        .create_run(
            &supervisor,
            CreateRun {
                project_id: project,
                goal: "complete".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    engine
        .complete_run(
            &supervisor,
            CompleteRun {
                run_id: run.id,
                outstanding_integration_steps: vec![],
            },
            IdempotencyKey("complete".into()),
        )
        .unwrap();
    assert!(human.0.lock().unwrap().contains(&TransitionContext {
        project_id: project,
        run_id: run.id,
        task_id: None,
        from: Some(LifecycleState::Run(RunStatus::Active)),
        to: Some(LifecycleState::Run(RunStatus::Completed)),
        identity: TransitionIdentity::RunCompletion,
    }));
}

#[test]
fn coder_cannot_create_run() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let ctx = context(Role::Coder);
    let project = seed_project(&db, &context(Role::Supervisor));
    assert!(matches!(
        engine(db).create_run(
            &ctx,
            CreateRun {
                project_id: project,
                goal: "x".into()
            },
            IdempotencyKey("k".into())
        ),
        Err(CoreError::Forbidden)
    ));
}

#[test]
fn comparison_selects_only_a_review_complete_tip_and_preserves_both_submissions() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let engine = engine(db.clone());
    let key = |name: &str| IdempotencyKey(name.into());
    let request = CreateComparisonRun {
        project_id: seed_project(&db, &supervisor),
        goal: "ship feature".into(),
        workflow: ComparisonWorkflow {
            candidates: 2,
            reviewers_per_candidate: 2,
            comparison_reviewers: 1,
            winner_approval: WinnerApproval::Human,
        },
    };
    let created = engine
        .create_comparison_run(&supervisor, request.clone(), key("create"))
        .unwrap();
    let replayed = engine
        .create_comparison_run(&supervisor, request.clone(), key("create"))
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(created.value, replayed.value);
    let [first, second] = created.value.comparison.as_ref().unwrap().candidates;
    assert_ne!(first, second);
    for candidate in [first, second] {
        let record = engine.load_task(candidate).unwrap().0;
        assert_eq!(record.task.goal, request.goal);
        assert!(record.dependencies.is_empty());
    }
    let run_id = created.value.id;
    let completion = CompleteRun {
        run_id,
        outstanding_integration_steps: Vec::new(),
    };
    assert!(matches!(
        engine.complete_run(&supervisor, completion.clone(), key("complete")),
        Err(CoreError::Recoverable(_))
    ));
    assert!(matches!(
        engine.create_task(
            &supervisor,
            CreateTask {
                run_id,
                title: "third".into(),
                goal: "not a candidate".into(),
            },
            key("third"),
        ),
        Err(CoreError::Invalid(_))
    ));

    let repo = RepositoryId::new();
    let commit = |sha: &str| CommitRef {
        repository_id: repo,
        commit_id: sha.into(),
    };
    let winning_refs = vec![commit("first-older"), commit("first-tip")];
    let losing_refs = vec![commit("second-tip")];
    let submit = |task_id: TaskId, commits: Vec<CommitRef>| {
        let actor = seed_coder(&db, &supervisor);
        let coder = SessionContext::validated(SessionId::new(), actor.id, Role::Coder);
        engine
            .claim_task(&coder, ClaimTask { task_id }, key("assign"))
            .unwrap();
        engine
            .claim_task(&coder, ClaimTask { task_id }, key("claim"))
            .unwrap();
        engine
            .submit_task(
                &coder,
                SubmitTask {
                    task_id,
                    commits,
                    summary: "submitted".into(),
                },
                key("submit"),
            )
            .unwrap();
    };
    submit(first, winning_refs.clone());
    submit(second, losing_refs.clone());

    let selection = SelectCandidate {
        run_id,
        task_id: first,
        commit_id: "first-tip".into(),
        reason: "approved after comparison".into(),
    };
    assert!(matches!(
        engine.select_candidate(&supervisor, selection.clone(), key("select")),
        Err(CoreError::Recoverable(_))
    ));
    let dispose = |task_id, outcome, name: &str| {
        engine
            .record_task_disposition(
                &supervisor,
                FinalTaskDisposition {
                    task_id,
                    outcome,
                    reason: name.into(),
                },
                key(name),
            )
            .unwrap();
    };
    dispose(first, FinalTaskOutcome::Completed, "review complete");
    assert!(matches!(
        engine.select_candidate(&supervisor, selection.clone(), key("select")),
        Err(CoreError::Recoverable(_))
    ));
    dispose(second, FinalTaskOutcome::Failed, "review retries exhausted");
    assert!(matches!(
        engine.select_candidate(
            &supervisor,
            SelectCandidate {
                commit_id: "first-older".into(),
                ..selection.clone()
            },
            key("select"),
        ),
        Err(CoreError::Invalid(_))
    ));
    let selected = engine
        .select_candidate(&supervisor, selection.clone(), key("select"))
        .unwrap();
    assert!(matches!(
        engine.select_candidate(
            &supervisor,
            SelectCandidate {
                task_id: second,
                commit_id: "second-tip".into(),
                ..selection.clone()
            },
            key("replacement"),
        ),
        Err(CoreError::Invalid(_))
    ));
    let replayed = engine
        .select_candidate(
            &context(Role::Supervisor),
            selection,
            key("recovered selection"),
        )
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(selected.event_id, replayed.event_id);
    let completed = engine
        .complete_run(&supervisor, completion, key("complete"))
        .unwrap();
    assert_eq!(completed.value.status, RunStatus::Completed);
    assert_eq!(completed.value.reviewed_commits, winning_refs);
    assert_eq!(
        engine.load_task(second).unwrap().0.task.submitted_commits,
        losing_refs
    );
}

struct ZeroBudgetPolicy;
impl ProjectWorkflowPolicy for ZeroBudgetPolicy {
    fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 2,
            max_task_retries: 0,
            max_review_rounds: 1,
        })
    }

    fn comparison_policies(&self, project_id: ProjectId) -> CoreResult<(WorkflowLimits, u32)> {
        Ok((self.workflow_limits(project_id)?, 2))
    }
}

fn zero_engine(db: Database) -> TaskEngine {
    TaskEngine::new(
        db,
        Arc::new(ZeroBudgetPolicy),
        Arc::new(Reviews),
        Arc::new(Human::default()),
        Arc::new(Leases),
    )
}

fn failed_candidate(
    engine: &TaskEngine,
    db: &Database,
    supervisor: &SessionContext,
) -> (Run, TaskId) {
    let run = engine
        .create_comparison_run(
            supervisor,
            CreateComparisonRun {
                project_id: seed_project(db, supervisor),
                goal: "repair".into(),
                workflow: ComparisonWorkflow {
                    candidates: 2,
                    reviewers_per_candidate: 2,
                    comparison_reviewers: 1,
                    winner_approval: WinnerApproval::Human,
                },
            },
            IdempotencyKey("create".into()),
        )
        .unwrap()
        .value;
    let candidate = run.comparison.as_ref().unwrap().candidates[0];
    let coder = seed_coder(db, supervisor);
    let coder = SessionContext::validated(SessionId::new(), coder.id, Role::Coder);
    engine
        .assign_task(
            supervisor,
            AssignTask {
                task_id: candidate,
                assignee: coder.actor_id(),
            },
            IdempotencyKey("assign".into()),
        )
        .unwrap();
    engine
        .claim_task(
            &coder,
            ClaimTask { task_id: candidate },
            IdempotencyKey("claim".into()),
        )
        .unwrap();
    engine
        .record_task_disposition(
            supervisor,
            FinalTaskDisposition {
                task_id: candidate,
                outcome: FinalTaskOutcome::Failed,
                reason: "budget".into(),
            },
            IdempotencyKey("fail".into()),
        )
        .unwrap();
    (run, candidate)
}

#[test]
fn repair_retry_once_extends_zero_budget_and_reopens_task() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let engine = zero_engine(db.clone());
    let (run, task_id) = failed_candidate(&engine, &db, &supervisor);
    let repaired = engine
        .repair_task(
            &supervisor,
            RepairTask {
                run_id: run.id,
                task_id,
                mode: RepairMode::RetryOnce,
                reason: "one more round".into(),
            },
            IdempotencyKey("repair-retry".into()),
        )
        .unwrap()
        .value;
    assert_eq!(repaired.task.status, TaskStatus::RevisionRequired);
    assert_eq!(repaired.task.attempts, 1);
    assert_eq!(repaired.run.status, RunStatus::Active);
    assert_eq!(repaired.run.limits.max_task_retries, 1);
    assert_eq!(repaired.run.limits.max_review_rounds, 1);
    let coder = seed_coder(&db, &supervisor);
    let coder = SessionContext::validated(SessionId::new(), coder.id, Role::Coder);
    engine
        .assign_task(
            &supervisor,
            AssignTask {
                task_id,
                assignee: coder.actor_id(),
            },
            IdempotencyKey("assign-again".into()),
        )
        .unwrap();
    let claimed = engine
        .claim_task(
            &coder,
            ClaimTask { task_id },
            IdempotencyKey("claim-again".into()),
        )
        .unwrap()
        .value;
    assert_eq!(claimed.attempts, 2);
}

#[test]
fn repair_reset_attempts_restores_a_fresh_attempt_budget() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let supervisor = context(Role::Supervisor);
    let engine = zero_engine(db.clone());
    let (run, task_id) = failed_candidate(&engine, &db, &supervisor);
    let repaired = engine
        .repair_task(
            &supervisor,
            RepairTask {
                run_id: run.id,
                task_id,
                mode: RepairMode::ResetAttempts,
                reason: "reset and retry".into(),
            },
            IdempotencyKey("repair-reset".into()),
        )
        .unwrap()
        .value;
    assert_eq!(repaired.task.attempts, 0);
    assert_eq!(repaired.task.status, TaskStatus::RevisionRequired);
    let coder = seed_coder(&db, &supervisor);
    let coder = SessionContext::validated(SessionId::new(), coder.id, Role::Coder);
    engine
        .assign_task(
            &supervisor,
            AssignTask {
                task_id,
                assignee: coder.actor_id(),
            },
            IdempotencyKey("assign-after-reset".into()),
        )
        .unwrap();
    let claimed = engine
        .claim_task(
            &coder,
            ClaimTask { task_id },
            IdempotencyKey("claim-after-reset".into()),
        )
        .unwrap()
        .value;
    assert_eq!(claimed.attempts, 1);
}
