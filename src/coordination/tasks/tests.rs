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
}
struct Reviews;
impl ReviewCompletionGate for Reviews {
    fn ensure_task_complete(&self, _: TaskId) -> CoreResult<()> {
        Ok(())
    }
    fn reviewed_commits_for_run(&self, _: RunId) -> CoreResult<Vec<String>> {
        Ok(Vec::new())
    }
}
struct Human;
impl HumanTransitionGate for Human {
    fn ensure_allowed(
        &self,
        _: &SessionContext,
        _: Option<TaskId>,
        _: Option<TaskStatus>,
        _: Option<TaskStatus>,
    ) -> CoreResult<()> {
        Ok(())
    }
}
struct Leases;
impl AssignmentLeaseStatus for Leases {
    fn is_expired(&self, _: TaskId, _: ActorId) -> CoreResult<bool> {
        Ok(false)
    }
}
fn context(role: Role) -> SessionContext {
    SessionContext::validated(SessionId::new(), ActorId::new(), role)
}
fn engine(db: Database) -> TaskEngine {
    TaskEngine::new(
        db,
        Arc::new(Policy),
        Arc::new(Reviews),
        Arc::new(Human),
        Arc::new(Leases),
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
                commit_ids: vec!["abc123".into()],
                summary: "done".into(),
            },
            IdempotencyKey("submit".into()),
        )
        .unwrap();
    engine
        .transition_for_review(
            &ctx,
            ReviewTaskTransition {
                task_id: prerequisite.id,
                status: TaskStatus::InReview,
                reason: "review requested".into(),
            },
            IdempotencyKey("review-state".into()),
        )
        .unwrap();
    engine
        .transition_for_review(
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
                commit_ids: vec!["abc124".into()],
                summary: "revised".into(),
            },
            IdempotencyKey("revision-submit".into()),
        )
        .unwrap();
    engine
        .transition_for_review(
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
