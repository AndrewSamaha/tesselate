use super::*;
use crate::coordination::tasks::{
    HumanTransitionGate, ProjectWorkflowPolicy, ReviewCompletionGate, TaskEngine, TransitionContext,
};
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration as StdDuration,
};

fn credential_secret(credential: &IssuedCredential) -> &str {
    credential
        .secret
        .as_deref()
        .expect("first issuance returns secret")
}

struct TempDb(PathBuf);
impl TempDb {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("tesselate-f05-{}", Uuid::new_v4()));
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

struct Fixture {
    temp: TempDb,
    db: Database,
    manager: Arc<SessionManager>,
    bootstrap_secret: String,
    supervisor_credential: IssuedCredential,
    supervisor: SessionContext,
    coder_credential: IssuedCredential,
    coder: SessionContext,
    reviewer_credential: IssuedCredential,
    reviewer: SessionContext,
    cli: SessionContext,
}

fn fixture() -> Fixture {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let manager = Arc::new(SessionManager::new(db.clone()));
    let bootstrap = manager.prepare_bootstrap().unwrap();
    let supervisor_credential = manager
        .bootstrap_supervisor(
            &bootstrap,
            "supervisor",
            IdempotencyKey("supervisor-bootstrap".into()),
        )
        .unwrap();
    let supervisor = manager
        .authenticate(credential_secret(&supervisor_credential))
        .unwrap();
    let coder_credential = manager
        .issue_agent(
            &supervisor,
            "coder",
            AgentRole::Coder,
            IdempotencyKey("issue-coder".into()),
        )
        .unwrap();
    let reviewer_credential = manager
        .issue_agent(
            &supervisor,
            "reviewer",
            AgentRole::Reviewer,
            IdempotencyKey("issue-reviewer".into()),
        )
        .unwrap();
    let cli_credential = manager
        .issue_cli(&supervisor, "cli", IdempotencyKey("issue-cli".into()))
        .unwrap();
    let coder = manager
        .authenticate(credential_secret(&coder_credential))
        .unwrap();
    let reviewer = manager
        .authenticate(credential_secret(&reviewer_credential))
        .unwrap();
    let cli = manager
        .authenticate(credential_secret(&cli_credential))
        .unwrap();
    Fixture {
        temp,
        db,
        manager,
        bootstrap_secret: bootstrap,
        supervisor_credential,
        supervisor,
        coder_credential,
        coder,
        reviewer_credential,
        reviewer,
        cli,
    }
}

struct Reviews;
impl ReviewCompletionGate for Reviews {
    fn ensure_task_complete(&self, _: TaskId) -> CoreResult<Vec<StatePrecondition>> {
        Ok(vec![])
    }
    fn reviewed_commits_for_run(&self, _: RunId) -> CoreResult<Vec<CommitRef>> {
        Ok(vec![])
    }
}
struct Workflow;
impl ProjectWorkflowPolicy for Workflow {
    fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 5,
            max_task_retries: 3,
            max_review_rounds: 3,
        })
    }
}
struct Human;
impl HumanTransitionGate for Human {
    fn ensure_allowed(&self, _: &SessionContext, _: TransitionContext) -> CoreResult<()> {
        Ok(())
    }
}
fn task_engine(f: &Fixture, leases: Arc<dyn AssignmentLeaseStatus>) -> TaskEngine {
    TaskEngine::new(
        f.db.clone(),
        Arc::new(Workflow),
        Arc::new(Reviews),
        Arc::new(Human),
        leases,
    )
}

#[test]
fn bootstrap_issues_only_one_supervisor_and_binds_role_to_credential() {
    let f = fixture();
    assert_ne!(f.supervisor.role(), f.coder.role());
    assert_eq!(f.supervisor.actor_id(), f.supervisor_credential.actor_id);
    assert_eq!(
        f.manager
            .authenticate(&credential_secret(&f.coder_credential))
            .unwrap()
            .role(),
        Role::Coder
    );
    assert!(matches!(
        f.manager.authenticate("supervisor"),
        Err(CoreError::Unauthenticated)
    ));
    assert!(matches!(
        f.manager.bootstrap_supervisor(
            "wrong",
            "attacker",
            IdempotencyKey("wrong-bootstrap".into())
        ),
        Err(CoreError::Unauthenticated)
    ));
    let bootstrap_retry = f
        .manager
        .bootstrap_supervisor(
            &f.bootstrap_secret,
            "supervisor",
            IdempotencyKey("supervisor-bootstrap".into()),
        )
        .unwrap();
    assert!(bootstrap_retry.replayed && bootstrap_retry.secret.is_none());
    let first_issue = f
        .manager
        .issue_agent(
            &f.supervisor,
            "retry coder",
            AgentRole::Coder,
            IdempotencyKey("retry-issue".into()),
        )
        .unwrap();
    let event_count = f.db.event_count().unwrap();
    let issue_retry = f
        .manager
        .issue_agent(
            &f.supervisor,
            "retry coder",
            AgentRole::Coder,
            IdempotencyKey("retry-issue".into()),
        )
        .unwrap();
    assert_eq!(issue_retry.session_id, first_issue.session_id);
    assert!(issue_retry.replayed && issue_retry.secret.is_none());
    assert_eq!(f.db.event_count().unwrap(), event_count);
    assert!(f
        .manager
        .authenticate(&credential_secret(&first_issue))
        .is_ok());
    assert!(matches!(
        f.manager.prepare_bootstrap(),
        Err(CoreError::Invalid(_))
    ));
    let forged =
        SessionContext::validated(f.coder.session_id(), f.coder.actor_id(), Role::Supervisor);
    assert!(matches!(
        f.manager
            .renew_session(&forged, IdempotencyKey("forged-renewal".into())),
        Err(CoreError::Unauthenticated)
    ));
    let events =
        f.db.events_after(
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None,
            },
            None,
            100,
        )
        .unwrap();
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(!serialized.contains(&credential_secret(&f.supervisor_credential)));
    assert!(!serialized.contains(&credential_secret(&f.coder_credential)));
    assert!(!format!("{:?}", f.coder_credential).contains(&credential_secret(&f.coder_credential)));
    let credentials =
        f.db.list_entities::<CredentialRecord>(
            EntityKind::Credential,
            &EntityScope::default(),
            PageRequest {
                limit: 100,
                after: None,
            },
        )
        .unwrap();
    assert!(!serde_json::to_string(&credentials)
        .unwrap()
        .contains(&credential_secret(&f.coder_credential)));
}

#[test]
fn revocation_expiry_and_restart_reconnect_are_durable() {
    let f = fixture();
    let reopened = Database::open(&f.temp.0).unwrap();
    let manager = SessionManager::new(reopened.clone());
    let reconnected = manager
        .authenticate(&credential_secret(&f.coder_credential))
        .unwrap();
    assert_eq!(reconnected.session_id(), f.coder.session_id());
    let renewed = manager
        .renew_session(&reconnected, IdempotencyKey("renew-session".into()))
        .unwrap();
    let renewal_replay = manager
        .renew_session(&reconnected, IdempotencyKey("renew-session".into()))
        .unwrap();
    assert!(renewal_replay.replayed);
    assert_eq!(renewal_replay.value, renewed.value);
    assert!(renewed.value > Utc::now());
    manager
        .revoke(
            &reconnected,
            f.reviewer.session_id(),
            IdempotencyKey("forbidden-revoke".into()),
        )
        .unwrap_err();
    f.manager
        .revoke(
            &f.supervisor,
            f.reviewer.session_id(),
            IdempotencyKey("revoke-reviewer".into()),
        )
        .unwrap();
    f.manager
        .revoke(
            &f.supervisor,
            f.reviewer.session_id(),
            IdempotencyKey("terminal-revoke".into()),
        )
        .unwrap();
    f.manager
        .revoke(
            &f.supervisor,
            f.reviewer.session_id(),
            IdempotencyKey("terminal-revoke".into()),
        )
        .unwrap();
    assert!(matches!(
        f.manager.revoke(
            &f.supervisor,
            f.coder.session_id(),
            IdempotencyKey("terminal-revoke".into()),
        ),
        Err(CoreError::IdempotencyConflict)
    ));
    f.manager
        .revoke(
            &f.supervisor,
            f.reviewer.session_id(),
            IdempotencyKey("revoke-reviewer".into()),
        )
        .unwrap();
    assert!(matches!(
        manager.authenticate(&credential_secret(&f.reviewer_credential)),
        Err(CoreError::Unauthenticated)
    ));
    assert!(matches!(
        manager.renew_session(&f.reviewer, IdempotencyKey("revoked-renew".into())),
        Err(CoreError::Unauthenticated)
    ));
    let short_temp = TempDb::new();
    let short = SessionManager::with_ttls(
        Database::open(&short_temp.0).unwrap(),
        Duration::milliseconds(200),
        Duration::minutes(1),
    )
    .unwrap();
    let bootstrap = short.prepare_bootstrap().unwrap();
    let admin = short
        .bootstrap_supervisor(
            &bootstrap,
            "short-lived supervisor",
            IdempotencyKey("short-bootstrap".into()),
        )
        .unwrap();
    let short_admin = short.authenticate(&credential_secret(&admin)).unwrap();
    let explicitly_expired = short
        .issue_agent(
            &short_admin,
            "explicitly expired",
            AgentRole::Coder,
            IdempotencyKey("explicit-expire-issue".into()),
        )
        .unwrap();
    short
        .expire_session(
            &short_admin,
            explicitly_expired.session_id,
            IdempotencyKey("expire-session".into()),
        )
        .unwrap();
    short
        .expire_session(
            &short_admin,
            explicitly_expired.session_id,
            IdempotencyKey("terminal-expire".into()),
        )
        .unwrap();
    short
        .expire_session(
            &short_admin,
            explicitly_expired.session_id,
            IdempotencyKey("terminal-expire".into()),
        )
        .unwrap();
    assert!(matches!(
        short.authenticate(&credential_secret(&explicitly_expired)),
        Err(CoreError::Unauthenticated)
    ));
    let short_coder = short
        .issue_agent(
            &short_admin,
            "short coder",
            AgentRole::Coder,
            IdempotencyKey("short-issue".into()),
        )
        .unwrap();
    assert!(matches!(
        short.expire_session(
            &short_admin,
            short_coder.session_id,
            IdempotencyKey("terminal-expire".into()),
        ),
        Err(CoreError::IdempotencyConflict)
    ));
    thread::sleep(StdDuration::from_millis(220));
    assert!(matches!(
        short.authenticate(&credential_secret(&short_coder)),
        Err(CoreError::Unauthenticated)
    ));
}

#[test]
fn lease_generation_is_revision_fenced_against_reassignment_and_workspace_is_retained() {
    let f = fixture();
    let run = task_engine(&f, f.manager.clone())
        .create_run(
            &f.supervisor,
            CreateRun {
                project_id: project(&f).id,
                goal: "test lease".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    let task = task_engine(&f, f.manager.clone())
        .create_task(
            &f.supervisor,
            CreateTask {
                run_id: run.id,
                title: "task".into(),
                goal: "work".into(),
            },
            IdempotencyKey("task".into()),
        )
        .unwrap()
        .value;
    task_engine(&f, f.manager.clone())
        .assign_task(
            &f.supervisor,
            AssignTask {
                task_id: task.id,
                assignee: f.coder.actor_id(),
            },
            IdempotencyKey("assignment".into()),
        )
        .unwrap();
    let workspace = Workspace {
        id: WorkspaceId::new(),
        task_id: task.id,
        repository_id: RepositoryId::new(),
        base_commit: "base".into(),
        branch: "work/task".into(),
        path: "/tmp/task-worktree".into(),
        owner: f.coder.actor_id(),
    };
    seed(
        &f.db,
        &f.supervisor,
        EntityKind::Workspace,
        workspace.id.0.to_string(),
        EntityScope {
            task_id: Some(task.id.0.to_string()),
            owner_id: Some(f.coder.actor_id().0.to_string()),
            ..Default::default()
        },
        &workspace,
        "workspace.created",
    );
    let lease_manager = Arc::new(
        SessionManager::with_ttls(
            f.db.clone(),
            Duration::hours(12),
            Duration::milliseconds(20),
        )
        .unwrap(),
    );
    let first = lease_manager
        .renew_assignment_lease(&f.coder, task.id, IdempotencyKey("lease-first".into()))
        .unwrap()
        .value;
    assert_eq!(first.generation, 1);
    let (stored, revision) =
        f.db.load::<AssignmentLeaseRecord>(EntityKind::AssignmentLease, &first.assignment_id)
            .unwrap()
            .unwrap();
    assert_eq!(stored.session_id, f.coder.session_id());
    let fence = LeaseFence {
        task_id: task.id,
        owner_id: f.coder.actor_id(),
        condition: StatePrecondition {
            kind: EntityKind::AssignmentLease,
            id: first.assignment_id.clone(),
            expected_revision: revision,
        },
    };
    let second = lease_manager
        .renew_assignment_lease(&f.coder, task.id, IdempotencyKey("lease-second".into()))
        .unwrap()
        .value;
    assert_eq!(second.generation, 2);
    let lease_replay = lease_manager
        .renew_assignment_lease(&f.coder, task.id, IdempotencyKey("lease-second".into()))
        .unwrap();
    assert!(lease_replay.replayed);
    assert_eq!(lease_replay.value, second);
    assert!(matches!(
        f.db.commit_mutation(MutationInput {
            context: &f.supervisor,
            operation: "test.stale-lease-fence",
            idempotency_key: &IdempotencyKey("stale".into()),
            request: &task.id,
            preconditions: vec![fence.condition],
            result: &task,
            changes: vec![StateChange::update(
                EntityKind::Task,
                task.id.0.to_string(),
                EntityScope::default(),
                1,
                &task
            )
            .unwrap()],
            event: NewEvent::from_context(
                &f.supervisor,
                "test",
                EventScope {
                    project_id: None,
                    run_id: None,
                    task_id: None
                },
                "test.stale",
                json!({})
            ),
        }),
        Err(PersistenceError::StateConflict(_))
    ));
    thread::sleep(StdDuration::from_millis(30));
    assert!(lease_manager
        .recoverable_leases(
            &f.supervisor,
            run.project_id,
            PageRequest {
                limit: 20,
                after: None
            }
        )
        .unwrap()
        .items
        .iter()
        .any(|lease| lease.assignment_id == second.assignment_id));
    let current = Arc::new(RenewDuringFence {
        leases: lease_manager.clone(),
        session: f.coder.clone(),
        renewed: AtomicBool::new(false),
    });
    let racer = TaskEngine::new(
        f.db.clone(),
        Arc::new(Workflow),
        Arc::new(Reviews),
        Arc::new(Human),
        current,
    );
    let next_coder = f
        .manager
        .issue_agent(
            &f.supervisor,
            "next coder",
            AgentRole::Coder,
            IdempotencyKey("next-coder".into()),
        )
        .unwrap();
    assert!(matches!(
        racer.assign_task(
            &f.supervisor,
            AssignTask {
                task_id: task.id,
                assignee: next_coder.actor_id
            },
            IdempotencyKey("race-assign".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    assert_eq!(
        f.db.load::<TaskRecord>(EntityKind::Task, &task.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .owner,
        Some(f.coder.actor_id())
    );
    thread::sleep(StdDuration::from_millis(30));
    task_engine(&f, lease_manager)
        .assign_task(
            &f.supervisor,
            AssignTask {
                task_id: task.id,
                assignee: next_coder.actor_id,
            },
            IdempotencyKey("reassign".into()),
        )
        .unwrap();
    assert_eq!(
        f.db.load::<TaskRecord>(EntityKind::Task, &task.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .task
            .owner,
        Some(next_coder.actor_id)
    );
    let (survivor, _) =
        f.db.load::<Workspace>(EntityKind::Workspace, &workspace.id.0.to_string())
            .unwrap()
            .unwrap();
    assert_eq!(survivor, workspace);
}

struct RenewDuringFence {
    leases: Arc<SessionManager>,
    session: SessionContext,
    renewed: AtomicBool,
}
impl AssignmentLeaseStatus for RenewDuringFence {
    fn expired_lease_fence(&self, task_id: TaskId, owner: ActorId) -> CoreResult<LeaseFence> {
        let fence = self.leases.expired_lease_fence(task_id, owner)?;
        if !self.renewed.swap(true, Ordering::SeqCst) {
            self.leases.renew_assignment_lease(
                &self.session,
                task_id,
                IdempotencyKey("racing-renewal".into()),
            )?;
        }
        Ok(fence)
    }
}

#[test]
fn messages_resume_after_restart_in_order_and_cli_cannot_send_them() {
    let f = fixture();
    let project = project(&f);
    let run = task_engine(&f, f.manager.clone())
        .create_run(
            &f.supervisor,
            CreateRun {
                project_id: project.id,
                goal: "messaging".into(),
            },
            IdempotencyKey("run".into()),
        )
        .unwrap()
        .value;
    let task = task_engine(&f, f.manager.clone())
        .create_task(
            &f.supervisor,
            CreateTask {
                run_id: run.id,
                title: "task".into(),
                goal: "work".into(),
            },
            IdempotencyKey("task".into()),
        )
        .unwrap()
        .value;
    task_engine(&f, f.manager.clone())
        .assign_task(
            &f.supervisor,
            AssignTask {
                task_id: task.id,
                assignee: f.coder.actor_id(),
            },
            IdempotencyKey("assign".into()),
        )
        .unwrap();
    assert!(matches!(
        f.manager.send_message(
            &f.cli,
            SendMessage {
                project_id: project.id,
                run_id: None,
                task_id: None,
                body: "denied".into()
            },
            IdempotencyKey("cli-message".into())
        ),
        Err(CoreError::Forbidden)
    ));
    f.manager
        .send_message(
            &f.coder,
            SendMessage {
                project_id: project.id,
                run_id: Some(run.id),
                task_id: Some(task.id),
                body: "first".into(),
            },
            IdempotencyKey("message-1".into()),
        )
        .unwrap();
    let events =
        f.db.events_after(
            EventScope {
                project_id: Some(project.id),
                run_id: None,
                task_id: None,
            },
            None,
            100,
        )
        .unwrap();
    let event_text = serde_json::to_string(&events).unwrap();
    assert!(!event_text.contains("first"));
    assert!(!event_text.contains("second"));
    let first_page = f
        .manager
        .read_messages(
            &f.coder,
            ReadMessages {
                project_id: project.id,
                task_id: Some(task.id),
                page: PageRequest {
                    limit: 1,
                    after: None,
                },
            },
        )
        .unwrap();
    assert_eq!(first_page.items[0].body, "first");
    let cursor = first_page.next_cursor.unwrap();
    f.manager
        .send_message(
            &f.reviewer,
            SendMessage {
                project_id: project.id,
                run_id: Some(run.id),
                task_id: Some(task.id),
                body: "second".into(),
            },
            IdempotencyKey("message-2".into()),
        )
        .unwrap_err();
    // Reviewer messages are limited to tasks currently under review.
    f.manager
        .send_message(
            &f.supervisor,
            SendMessage {
                project_id: project.id,
                run_id: Some(run.id),
                task_id: Some(task.id),
                body: "second".into(),
            },
            IdempotencyKey("message-2".into()),
        )
        .unwrap();
    let reopened = SessionManager::new(Database::open(&f.temp.0).unwrap());
    let reconnected = reopened
        .authenticate(&credential_secret(&f.coder_credential))
        .unwrap();
    let next = reopened
        .read_messages(
            &reconnected,
            ReadMessages {
                project_id: project.id,
                task_id: Some(task.id),
                page: PageRequest {
                    limit: 1,
                    after: Some(EventCursor(cursor)),
                },
            },
        )
        .unwrap();
    assert_eq!(
        next.items
            .iter()
            .map(|message| message.body.as_str())
            .collect::<Vec<_>>(),
        vec!["second"]
    );
    let exhausted = reopened
        .read_messages(
            &reconnected,
            ReadMessages {
                project_id: project.id,
                task_id: Some(task.id),
                page: PageRequest {
                    limit: 1,
                    after: next.next_cursor.map(EventCursor),
                },
            },
        )
        .unwrap();
    assert!(exhausted.items.is_empty());
}

fn project(f: &Fixture) -> Project {
    let project = Project {
        id: ProjectId::new(),
        name: "project".into(),
    };
    seed(
        &f.db,
        &f.supervisor,
        EntityKind::Project,
        project.id.0.to_string(),
        EntityScope::default(),
        &project,
        "project.created",
    );
    project
}
fn seed<T: Serialize + for<'de> Deserialize<'de>>(
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
        operation: "test.seed",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: value,
        preconditions: vec![],
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
