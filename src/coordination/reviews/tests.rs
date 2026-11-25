use super::*;
use crate::coordination::tasks::{
    AssignmentLeaseStatus, HumanTransitionGate, LeaseFence, ProjectWorkflowPolicy,
    ReviewCompletionGate, TaskRecord, TransitionContext,
};
use crate::persistence::{MutationInput, NewEvent, StateChange, StatePrecondition};
use chrono::Utc;
use rusqlite::Connection;
use serde::de::DeserializeOwned;
use std::{fs, path::PathBuf};
use uuid::Uuid;

struct TempDb(PathBuf);
impl TempDb {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("tesselate-f06-{}", Uuid::new_v4()));
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
struct Policy {
    minimum: u32,
}
impl ProjectReviewPolicy for Policy {
    fn review_policy(&self, _: ProjectId) -> CoreResult<ReviewPolicy> {
        Ok(ReviewPolicy {
            min_independent_reviewers: self.minimum,
            high_severity_defer_requires_human: false,
        })
    }
}
struct Workflow;
impl ProjectWorkflowPolicy for Workflow {
    fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 3,
            max_task_retries: 2,
            max_review_rounds: 3,
        })
    }

    fn comparison_policies(&self, project_id: ProjectId) -> CoreResult<(WorkflowLimits, u32)> {
        Ok((self.workflow_limits(project_id)?, 2))
    }
}
struct Authors(HashSet<ActorId>);
impl CommitAuthorLookup for Authors {
    fn authors(&self, _: &[CommitRef]) -> CoreResult<HashSet<ActorId>> {
        Ok(self.0.clone())
    }

    fn ensure_comparison_capture(&self, _: TaskId, _: &[CommitRef]) -> CoreResult<()> {
        Err(CoreError::Invalid("fixture has no comparison captures".into()))
    }
}
struct Human;
impl ReviewHumanGate for Human {
    fn allow_high_severity_deferral(
        &self,
        _: &SessionContext,
        _: ProjectId,
        _: RunId,
        _: TaskId,
        _: &ReviewFinding,
        _: &str,
    ) -> CoreResult<()> {
        Ok(())
    }
}
struct TaskHuman;
impl HumanTransitionGate for TaskHuman {
    fn ensure_allowed(&self, _: &SessionContext, _: TransitionContext) -> CoreResult<()> {
        Ok(())
    }
}
struct Leases;
impl AssignmentLeaseStatus for Leases {
    fn expired_lease_fence(&self, _: TaskId, _: ActorId) -> CoreResult<LeaseFence> {
        Err(CoreError::Recoverable("not expired".into()))
    }
}

struct Fixture {
    temp: TempDb,
    db: Database,
    task: Task,
    supervisor: SessionContext,
    reviewers: Vec<Actor>,
    reviewer_contexts: Vec<SessionContext>,
    initial_commit: CommitRef,
}
impl Fixture {
    fn new(
        minimum_reviewers: u32,
        author_ids: HashSet<ActorId>,
    ) -> (Self, Arc<ReviewCoordinator>, TaskEngine) {
        let temp = TempDb::new();
        let db = Database::open(&temp.0).unwrap();
        let supervisor = context(Role::Supervisor);
        let supervisor_actor = Actor {
            id: supervisor.actor_id(),
            display_name: "supervisor".into(),
            role: Role::Supervisor,
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Actor,
            supervisor_actor.id.0.to_string(),
            EntityScope::default(),
            &supervisor_actor,
            "actor.created",
        );
        let project = Project {
            id: ProjectId::new(),
            name: "fixture".into(),
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
        let repository = Repository {
            id: RepositoryId::new(),
            project_id: project.id,
            name: "repo".into(),
            canonical_path: "/tmp/repo".into(),
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Repository,
            repository.id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                ..Default::default()
            },
            &repository,
            "repository.registered",
        );
        let coder = actor(&db, &supervisor, Role::Coder, "coder");
        let reviewers = vec![
            actor(&db, &supervisor, Role::Reviewer, "reviewer-one"),
            actor(&db, &supervisor, Role::Reviewer, "reviewer-two"),
            actor(&db, &supervisor, Role::Reviewer, "reviewer-three"),
        ];
        let reviewer_contexts = reviewers
            .iter()
            .map(|reviewer| {
                SessionContext::validated(SessionId::new(), reviewer.id, Role::Reviewer)
            })
            .collect();
        let run = Run {
            id: RunId::new(),
            project_id: project.id,
            goal: "goal".into(),
            started_at: Utc::now(),
            limits: WorkflowLimits {
                max_duration_hours: 24,
                max_concurrent_assignments: 3,
                max_task_retries: 2,
                max_review_rounds: 3,
            },
            status: RunStatus::Active,
            reviewed_commits: Vec::new(),
            outstanding_integration_steps: Vec::new(),
            outcome: None,
            comparison: None,
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
        let initial_commit = CommitRef {
            repository_id: repository.id,
            commit_id: "sha-1".into(),
        };
        let task = Task {
            id: TaskId::new(),
            run_id: run.id,
            title: "task".into(),
            goal: "do it".into(),
            status: TaskStatus::Submitted,
            owner: Some(coder.id),
            progress: None,
            attempts: 1,
            submitted_commits: vec![initial_commit.clone()],
            result: Some("submitted".into()),
        };
        let record = TaskRecord {
            task: task.clone(),
            dependencies: vec![],
            waiting_on_dependencies: false,
            final_disposition: None,
        };
        insert(
            &db,
            &supervisor,
            EntityKind::Task,
            task.id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                run_id: Some(run.id.0.to_string()),
                task_id: Some(task.id.0.to_string()),
                owner_id: Some(coder.id.0.to_string()),
            },
            &record,
            "task.submitted",
        );
        let reviews = Arc::new(ReviewCoordinator::new(
            db.clone(),
            Arc::new(Policy {
                minimum: minimum_reviewers,
            }),
            Arc::new(Authors(author_ids)),
            Arc::new(Human),
        ));
        let tasks = TaskEngine::new(
            db.clone(),
            Arc::new(Workflow),
            reviews.clone(),
            Arc::new(TaskHuman),
            Arc::new(Leases),
        );
        (
            Self {
                temp,
                db,
                task,
                supervisor,
                reviewers,
                reviewer_contexts,
                initial_commit,
            },
            reviews,
            tasks,
        )
    }
    fn request(&self, refs: Vec<CommitRef>, previous: Vec<FindingId>) -> ReviewRequest {
        ReviewRequest {
            task_id: self.task.id,
            commit_refs: refs,
            previous_finding_ids: previous,
        }
    }
    fn submit_pass(
        &self,
        reviews: &ReviewCoordinator,
        round: ReviewRoundId,
        reviewer_index: usize,
        key: &str,
    ) -> Mutation<Review> {
        reviews
            .submit_review(
                &self.reviewer_contexts[reviewer_index],
                SubmitReview {
                    round_id: round,
                    passed: true,
                    summary: "passes".into(),
                    findings: vec![],
                },
                IdempotencyKey(key.into()),
            )
            .unwrap()
    }
}
fn context(role: Role) -> SessionContext {
    SessionContext::validated(SessionId::new(), ActorId::new(), role)
}
fn insert<T: Serialize + DeserializeOwned>(
    db: &Database,
    ctx: &SessionContext,
    kind: EntityKind,
    id: String,
    entity_scope: EntityScope,
    value: &T,
    event_type: &str,
) {
    let change = StateChange::insert(kind, id, entity_scope, value).unwrap();
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.seed",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: value,
        preconditions: vec![],
        result: value,
        changes: vec![change],
        event: NewEvent::from_context(
            ctx,
            "seed",
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
fn actor(db: &Database, ctx: &SessionContext, role: Role, name: &str) -> Actor {
    let actor = Actor {
        id: ActorId::new(),
        display_name: name.into(),
        role,
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
    actor
}
fn coordinator(db: &Database, minimum: u32, authors: HashSet<ActorId>) -> Arc<ReviewCoordinator> {
    Arc::new(ReviewCoordinator::new(
        db.clone(),
        Arc::new(Policy { minimum }),
        Arc::new(Authors(authors)),
        Arc::new(Human),
    ))
}

fn page() -> PageRequest {
    PageRequest {
        limit: 100,
        after: None,
    }
}

fn engine_for(fixture: &Fixture, reviews: Arc<ReviewCoordinator>) -> TaskEngine {
    TaskEngine::new(
        fixture.db.clone(),
        Arc::new(Workflow),
        reviews,
        Arc::new(TaskHuman),
        Arc::new(Leases),
    )
}

#[test]
fn pending_round_is_atomic_replayable_and_readable_after_restart() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let request = fixture.request(vec![fixture.initial_commit.clone()], vec![]);
    let key = IdempotencyKey("round-one".into());
    let first = reviews
        .request_review(&fixture.supervisor, request.clone(), key.clone(), &tasks)
        .unwrap();
    let replay = reviews
        .request_review(&fixture.supervisor, request, key, &tasks)
        .unwrap();
    assert!(!first.replayed && replay.replayed);
    assert_eq!(first.event_id, replay.event_id);
    assert_eq!(
        fixture
            .db
            .load::<TaskRecord>(EntityKind::Task, &fixture.task.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .status,
        TaskStatus::InReview
    );
    assert!(reviews
        .list_reviews(
            &fixture.supervisor,
            ListReviews {
                task_id: fixture.task.id,
                page: page()
            }
        )
        .unwrap()
        .items
        .is_empty());
    let rounds = reviews
        .list_review_rounds(
            &fixture.supervisor,
            ListReviewRounds {
                task_id: fixture.task.id,
                page: page(),
            },
        )
        .unwrap();
    assert_eq!(rounds.items, vec![first.value.clone()]);
    drop(tasks);
    drop(reviews);
    let reopened = Database::open(&fixture.temp.0).unwrap();
    let reader = coordinator(&reopened, 2, HashSet::new());
    let stored = reader
        .list_review_rounds(
            &fixture.supervisor,
            ListReviewRounds {
                task_id: fixture.task.id,
                page: page(),
            },
        )
        .unwrap();
    assert_eq!(stored.items, vec![first.value]);
    assert_eq!(
        reopened.event_count().unwrap(),
        fixture.db.event_count().unwrap()
    );
}

#[test]
fn round_insert_failure_rolls_back_in_review_transition() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    Connection::open(&fixture.temp.0).unwrap().execute_batch("CREATE TRIGGER reject_round BEFORE INSERT ON review_rounds BEGIN SELECT RAISE(ABORT, 'test round failure'); END;").unwrap();
    let before = fixture.db.event_count().unwrap();
    let result = reviews.request_review(
        &fixture.supervisor,
        fixture.request(vec![fixture.initial_commit.clone()], vec![]),
        IdempotencyKey("failed-round".into()),
        &tasks,
    );
    assert!(matches!(result, Err(CoreError::Internal(_))));
    assert_eq!(
        fixture
            .db
            .load::<TaskRecord>(EntityKind::Task, &fixture.task.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .status,
        TaskStatus::Submitted
    );
    assert_eq!(fixture.db.event_count().unwrap(), before);
}

#[test]
fn self_review_and_missing_explicit_result_are_rejected() {
    let (fixture, _reviews, tasks) = Fixture::new(1, HashSet::new());
    drop(tasks);
    let authors = coordinator(&fixture.db, 1, HashSet::from([fixture.reviewers[0].id]));
    let task_engine = engine_for(&fixture, authors.clone());
    let round = authors
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round".into()),
            &task_engine,
        )
        .unwrap()
        .value;
    let no_result = authors.submit_review(
        &fixture.reviewer_contexts[1],
        SubmitReview {
            round_id: round.id,
            passed: false,
            summary: "no verdict".into(),
            findings: vec![],
        },
        IdempotencyKey("no-result".into()),
    );
    assert!(matches!(no_result, Err(CoreError::Invalid(_))));
    let self_review = authors.submit_review(
        &fixture.reviewer_contexts[0],
        SubmitReview {
            round_id: round.id,
            passed: true,
            summary: "self".into(),
            findings: vec![],
        },
        IdempotencyKey("self-review".into()),
    );
    assert!(matches!(self_review, Err(CoreError::Forbidden)));
}

#[test]
fn completion_requires_minimum_reviewers_and_disposition_of_every_finding() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let round = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round".into()),
            &tasks,
        )
        .unwrap()
        .value;
    fixture.submit_pass(&reviews, round.id, 0, "pass-1");
    assert!(matches!(
        reviews.ensure_task_complete(fixture.task.id),
        Err(CoreError::Recoverable(_))
    ));
    fixture.submit_pass(&reviews, round.id, 1, "pass-2");
    assert!(reviews.ensure_task_complete(fixture.task.id).is_ok());

    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let round = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round".into()),
            &tasks,
        )
        .unwrap()
        .value;
    let finding = ReviewFinding {
        id: FindingId::new(),
        severity: FindingSeverity::Medium,
        location: "src/main.rs:1".into(),
        evidence: "broken behavior".into(),
        proposed_correction: "fix it".into(),
    };
    reviews
        .submit_review(
            &fixture.reviewer_contexts[0],
            SubmitReview {
                round_id: round.id,
                passed: false,
                summary: "one finding".into(),
                findings: vec![finding],
            },
            IdempotencyKey("findings".into()),
        )
        .unwrap();
    fixture.submit_pass(&reviews, round.id, 1, "finding-second-reviewer");
    assert!(matches!(
        reviews.ensure_task_complete(fixture.task.id),
        Err(CoreError::Recoverable(_))
    ));
}

#[test]
fn fixed_finding_requires_a_distinct_revision_round_and_pending_rounds_survive_restart() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let round_one = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round-1".into()),
            &tasks,
        )
        .unwrap()
        .value;
    let finding = ReviewFinding {
        id: FindingId::new(),
        severity: FindingSeverity::Low,
        location: "src/lib.rs:2".into(),
        evidence: "needs change".into(),
        proposed_correction: "change it".into(),
    };
    reviews
        .submit_review(
            &fixture.reviewer_contexts[0],
            SubmitReview {
                round_id: round_one.id,
                passed: false,
                summary: "finding".into(),
                findings: vec![finding.clone()],
            },
            IdempotencyKey("finding".into()),
        )
        .unwrap();
    fixture.submit_pass(&reviews, round_one.id, 1, "finding-second-reviewer");
    let disposition = FindingDisposition {
        finding_id: finding.id,
        disposition: DispositionKind::Fixed,
        reason: "updated".into(),
    };
    reviews
        .disposition_findings(
            &fixture.supervisor,
            DispositionFindings {
                task_id: fixture.task.id,
                dispositions: vec![disposition],
            },
            IdempotencyKey("fixed".into()),
            &tasks,
        )
        .unwrap();

    let (mut task_record, revision) = fixture
        .db
        .load::<TaskRecord>(EntityKind::Task, &fixture.task.id.0.to_string())
        .unwrap()
        .unwrap();
    task_record.task.status = TaskStatus::Submitted;
    task_record.task.submitted_commits = vec![CommitRef {
        repository_id: fixture.initial_commit.repository_id,
        commit_id: "sha-2".into(),
    }];
    fixture
        .db
        .commit_mutation(MutationInput {
            context: &fixture.supervisor,
            operation: "test.revise_submission",
            idempotency_key: &IdempotencyKey("revision-submit".into()),
            request: &task_record.task.submitted_commits,
            preconditions: vec![],
            result: &task_record.task,
            changes: vec![StateChange::update(
                EntityKind::Task,
                fixture.task.id.0.to_string(),
                EntityScope {
                    task_id: Some(fixture.task.id.0.to_string()),
                    run_id: Some(fixture.task.run_id.0.to_string()),
                    ..Default::default()
                },
                revision,
                &task_record,
            )
            .unwrap()],
            event: NewEvent::from_context(
                &fixture.supervisor,
                "test",
                EventScope {
                    project_id: None,
                    run_id: None,
                    task_id: None,
                },
                "test.revision",
                json!({}),
            ),
        })
        .unwrap();
    let round_two = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(task_record.task.submitted_commits.clone(), vec![finding.id]),
            IdempotencyKey("round-2".into()),
            &tasks,
        )
        .unwrap()
        .value;
    assert_eq!(round_two.round, 2);
    assert_eq!(round_two.previous_finding_ids, vec![finding.id]);
    fixture.submit_pass(&reviews, round_two.id, 0, "revision-pass-1");
    fixture.submit_pass(&reviews, round_two.id, 1, "revision-pass-2");
    assert!(reviews.ensure_task_complete(fixture.task.id).is_ok());
    assert_eq!(
        reviews
            .list_review_rounds(
                &fixture.supervisor,
                ListReviewRounds {
                    task_id: fixture.task.id,
                    page: page()
                }
            )
            .unwrap()
            .items,
        vec![round_one, round_two]
    );
}

#[test]
fn successful_completion_is_replayable_and_late_reviews_are_rejected() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let round = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round".into()),
            &tasks,
        )
        .unwrap()
        .value;
    fixture.submit_pass(&reviews, round.id, 0, "pass");
    fixture.submit_pass(&reviews, round.id, 1, "pass-2");
    let disposition = FinalTaskDisposition {
        task_id: fixture.task.id,
        outcome: FinalTaskOutcome::Completed,
        reason: "reviewed".into(),
    };
    let first = tasks
        .record_task_disposition(
            &fixture.supervisor,
            disposition.clone(),
            IdempotencyKey("complete".into()),
        )
        .unwrap();
    let replay = tasks
        .record_task_disposition(
            &fixture.supervisor,
            disposition,
            IdempotencyKey("complete".into()),
        )
        .unwrap();
    assert!(!first.replayed && replay.replayed);
    assert_eq!(first.event_id, replay.event_id);
    assert_eq!(first.value.status, TaskStatus::Completed);
    assert!(matches!(
        reviews.submit_review(
            &fixture.reviewer_contexts[1],
            SubmitReview {
                round_id: round.id,
                passed: true,
                summary: "late".into(),
                findings: vec![]
            },
            IdempotencyKey("late-review".into())
        ),
        Err(CoreError::Invalid(_))
    ));
}

struct RacingReviews {
    inner: Arc<ReviewCoordinator>,
    ctx: SessionContext,
    round_id: ReviewRoundId,
}
impl ReviewCompletionGate for RacingReviews {
    fn ensure_task_complete(&self, task_id: TaskId) -> CoreResult<Vec<StatePrecondition>> {
        let guards = self.inner.ensure_task_complete(task_id)?;
        self.inner.submit_review(
            &self.ctx,
            SubmitReview {
                round_id: self.round_id,
                passed: true,
                summary: "racing result".into(),
                findings: vec![],
            },
            IdempotencyKey("racing-review".into()),
        )?;
        Ok(guards)
    }
    fn reviewed_commits_for_run(&self, run_id: RunId) -> CoreResult<Vec<CommitRef>> {
        self.inner.reviewed_commits_for_run(run_id)
    }
}

#[test]
fn a_review_submitted_between_gate_and_completion_fences_completion() {
    let (fixture, reviews, tasks) = Fixture::new(2, HashSet::new());
    let round = reviews
        .request_review(
            &fixture.supervisor,
            fixture.request(vec![fixture.initial_commit.clone()], vec![]),
            IdempotencyKey("round".into()),
            &tasks,
        )
        .unwrap()
        .value;
    fixture.submit_pass(&reviews, round.id, 0, "pass");
    fixture.submit_pass(&reviews, round.id, 1, "pass-2");
    let racing = Arc::new(RacingReviews {
        inner: reviews.clone(),
        ctx: fixture.reviewer_contexts[2].clone(),
        round_id: round.id,
    });
    let task_engine = TaskEngine::new(
        fixture.db.clone(),
        Arc::new(Workflow),
        racing,
        Arc::new(TaskHuman),
        Arc::new(Leases),
    );
    let result = task_engine.record_task_disposition(
        &fixture.supervisor,
        FinalTaskDisposition {
            task_id: fixture.task.id,
            outcome: FinalTaskOutcome::Completed,
            reason: "reviewed".into(),
        },
        IdempotencyKey("race-complete".into()),
    );
    assert!(matches!(result, Err(CoreError::Recoverable(_))));
    assert_ne!(
        fixture
            .db
            .load::<TaskRecord>(EntityKind::Task, &fixture.task.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .status,
        TaskStatus::Completed
    );
}
