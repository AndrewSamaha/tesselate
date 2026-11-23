use chrono::Utc;
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tesselate::{
    coordination::human::{HumanCoordinator, ProjectHumanGatePolicy},
    coordination::sessions::SessionManager,
    coordination::tasks::{AssignmentRecord, AssignmentStatus, TaskRecord},
    coordination::Coordination,
    core::*,
    mcp::{serve_coordinator, PROTOCOL_VERSION},
    persistence::{Database, EntityKind, EntityScope, MutationInput, NewEvent, StateChange},
    CredentialIssuer, SessionAuthenticator,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use uuid::Uuid;

macro_rules! unused_methods {
    ($($name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)+) => {
        $(fn $name(&self, $($arg: $ty),*) -> $ret { $(let _ = $arg;)* panic!("unused integration test operation") })+
    };
}

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("tesselate-mcp-test-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct PersistedCoordination {
    db: Database,
    human: HumanCoordinator,
}

struct NoHumanGates;
impl ProjectHumanGatePolicy for NoHumanGates {
    fn human_gate_policy(&self, _: ProjectId) -> CoreResult<Option<HumanGatePolicy>> {
        Ok(None)
    }
}

fn coordination(db: Database) -> PersistedCoordination {
    PersistedCoordination {
        human: HumanCoordinator::new(db.clone(), Arc::new(NoHumanGates)),
        db,
    }
}

impl Coordination for PersistedCoordination {
    fn list_projects(&self, _: &SessionContext, page: PageRequest) -> CoreResult<Page<Project>> {
        self.db
            .list_entities(EntityKind::Project, &EntityScope::default(), page)
            .map_err(internal)
    }
    fn list_runs(
        &self,
        _: &SessionContext,
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
            .map_err(internal)
    }
    fn list_tasks(
        &self,
        _: &SessionContext,
        run: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>> {
        let records: Page<TaskRecord> = self
            .db
            .list_entities(
                EntityKind::Task,
                &EntityScope {
                    run_id: Some(run.0.to_string()),
                    ..Default::default()
                },
                page,
            )
            .map_err(internal)?;
        Ok(Page {
            items: records
                .items
                .into_iter()
                .map(|record| record.task)
                .collect(),
            next_cursor: records.next_cursor,
        })
    }
    fn create_run(
        &self,
        ctx: &SessionContext,
        request: CreateRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>> {
        if ctx.role() != Role::Supervisor {
            return Err(CoreError::Forbidden);
        }
        let run = Run {
            id: RunId::new(),
            project_id: request.project_id,
            goal: request.goal.clone(),
            started_at: Utc::now(),
            limits: WorkflowLimits {
                max_duration_hours: 24,
                max_concurrent_assignments: 2,
                max_task_retries: 2,
                max_review_rounds: 2,
            },
            status: RunStatus::Active,
            reviewed_commits: vec![],
            outstanding_integration_steps: vec![],
            outcome: None,
        };
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: "runs.create",
                idempotency_key: &key,
                request: &request,
                preconditions: vec![],
                result: &run,
                changes: vec![StateChange::insert(
                    EntityKind::Run,
                    run.id.0.to_string(),
                    EntityScope {
                        project_id: Some(run.project_id.0.to_string()),
                        run_id: Some(run.id.0.to_string()),
                        ..Default::default()
                    },
                    &run,
                )
                .map_err(internal)?],
                event: NewEvent::from_context(
                    ctx,
                    "supervisor",
                    EventScope {
                        project_id: Some(run.project_id),
                        run_id: Some(run.id),
                        task_id: None,
                    },
                    "run.created",
                    json!({"run_id":run.id,"goal":run.goal}),
                ),
            })
            .map_err(internal)
    }
    fn timeline(
        &self,
        _: &SessionContext,
        scope: EventScope,
        page: PageRequest,
    ) -> CoreResult<Page<Event>> {
        self.db
            .events_after(scope, page.after.as_ref(), page.limit)
            .map_err(internal)
    }
    fn list_human_requests(
        &self,
        ctx: &SessionContext,
        request: ListHumanRequests,
    ) -> CoreResult<Page<HumanDecision>> {
        self.human.list_human_requests(ctx, request)
    }
    fn send_message(
        &self,
        ctx: &SessionContext,
        request: SendMessage,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Message>> {
        let event_id = EventId::new();
        let message = Message {
            id: MessageId::new(),
            project_id: request.project_id,
            run_id: request.run_id,
            task_id: request.task_id,
            sender: ctx.actor_id(),
            body: request.body.clone(),
            event_id,
        };
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: "messages.send",
                idempotency_key: &key,
                request: &request,
                preconditions: vec![],
                result: &message,
                changes: vec![StateChange::insert(
                    EntityKind::Message,
                    message.id.0.to_string(),
                    EntityScope {
                        project_id: Some(message.project_id.0.to_string()),
                        run_id: message.run_id.map(|id| id.0.to_string()),
                        task_id: message.task_id.map(|id| id.0.to_string()),
                        owner_id: Some(message.sender.0.to_string()),
                    },
                    &message,
                )
                .map_err(internal)?],
                event: NewEvent {
                    event_id,
                    occurred_at: Utc::now(),
                    actor: Actor {
                        id: ctx.actor_id(),
                        display_name: "test actor".into(),
                        role: ctx.role(),
                    },
                    project_id: Some(message.project_id),
                    run_id: message.run_id,
                    task_id: message.task_id,
                    event_type: "message.sent".into(),
                    payload: json!({"message_id":message.id,"sender":message.sender}),
                },
            })
            .map_err(internal)
    }
    fn read_messages(
        &self,
        _: &SessionContext,
        request: ReadMessages,
    ) -> CoreResult<Page<Message>> {
        self.db
            .messages_after(
                request.project_id,
                request.task_id,
                request.page.after.as_ref(),
                request.page.limit,
            )
            .map_err(internal)
    }
    unused_methods! {
        list_repositories(ctx: &SessionContext, project: ProjectId, page: PageRequest) -> CoreResult<Page<Repository>>;
        list_agents(ctx: &SessionContext, project: ProjectId, page: PageRequest) -> CoreResult<Page<Agent>>;
        available_work(ctx: &SessionContext, project: ProjectId, page: PageRequest) -> CoreResult<Page<Task>>;
        create_task(ctx: &SessionContext, request: CreateTask, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        create_task_with_dependencies(ctx: &SessionContext, request: CreateTaskWithDependencies, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        assign_task(ctx: &SessionContext, request: AssignTask, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        claim_task(ctx: &SessionContext, request: ClaimTask, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        release_task(ctx: &SessionContext, request: ReleaseTask, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        update_progress(ctx: &SessionContext, request: UpdateProgress, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        submit_task(ctx: &SessionContext, request: SubmitTask, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        record_task_disposition(ctx: &SessionContext, request: FinalTaskDisposition, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        complete_run(ctx: &SessionContext, request: CompleteRun, key: IdempotencyKey) -> CoreResult<Mutation<Run>>;
        request_workspace(ctx: &SessionContext, request: WorkspaceRequest, key: IdempotencyKey) -> CoreResult<Mutation<Workspace>>;
        request_review(ctx: &SessionContext, request: ReviewRequest, key: IdempotencyKey) -> CoreResult<Mutation<ReviewRound>>;
        submit_review(ctx: &SessionContext, request: SubmitReview, key: IdempotencyKey) -> CoreResult<Mutation<Review>>;
        list_reviews(ctx: &SessionContext, request: ListReviews) -> CoreResult<Page<Review>>;
        list_review_rounds(ctx: &SessionContext, request: ListReviewRounds) -> CoreResult<Page<ReviewRound>>;
        disposition_findings(ctx: &SessionContext, request: DispositionFindings, key: IdempotencyKey) -> CoreResult<Mutation<Task>>;
        request_human(ctx: &SessionContext, request: HumanRequest, key: IdempotencyKey) -> CoreResult<Mutation<HumanDecision>>;
        resolve_human(ctx: &SessionContext, request: ResolveHumanRequest, key: IdempotencyKey) -> CoreResult<Mutation<HumanDecision>>;
        inspect_workspace(ctx: &SessionContext, request: InspectWorkspace) -> CoreResult<WorkspaceInspection>;
        capture_commit_refs(ctx: &SessionContext, request: CaptureWorkspaceCommits, key: IdempotencyKey) -> CoreResult<Mutation<Vec<CommitRef>>>;
        cleanup_workspace(ctx: &SessionContext, request: CleanupWorkspace, key: IdempotencyKey) -> CoreResult<Mutation<Workspace>>;
        locate_artifacts(ctx: &SessionContext, run: RunId, page: PageRequest) -> CoreResult<Page<ArtifactRef>>;
    }
}

fn internal(error: impl std::fmt::Display) -> CoreError {
    CoreError::Internal(error.to_string())
}

struct StdioClient {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}
impl StdioClient {
    fn start(socket: &str, bearer: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_tesselate-mcp"))
            .env("TESSELATE_SOCKET", socket)
            .env("TESSELATE_CREDENTIAL", bearer)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self {
            child,
            input,
            output,
        };
        let response = client.request(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}));
        assert!(response["result"]["serverInfo"]["name"].as_str().is_some());
        client.notify(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        client
    }
    fn notify(&mut self, value: Value) {
        writeln!(self.input, "{value}").unwrap();
        self.input.flush().unwrap();
    }
    fn request(&mut self, value: Value) -> Value {
        self.notify(value);
        let mut line = String::new();
        self.output.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }
    fn tool(&mut self, tool: &str, args: Value, id: u64) -> Value {
        self.request(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":tool,"arguments":args}}))
    }
}
impl Drop for StdioClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn args(params: Value, key: Option<&str>, request_id: &str) -> Value {
    let mut envelope =
        json!({"schema_version":PROTOCOL_VERSION,"request_id":request_id,"params":params});
    if let Some(key) = key {
        envelope["idempotency_key"] = json!(key);
    }
    envelope
}
fn seed_assignment(
    db: &Database,
    ctx: &SessionContext,
    project_id: ProjectId,
    run_id: RunId,
    owner: ActorId,
) -> TaskId {
    let task_id = TaskId::new();
    let task = Task {
        id: task_id,
        run_id,
        title: "retained assignment".into(),
        goal: "persist through reconnect".into(),
        status: TaskStatus::Assigned,
        owner: Some(owner),
        progress: None,
        attempts: 1,
        submitted_commits: vec![],
        result: None,
    };
    let task_record = TaskRecord {
        task: task.clone(),
        dependencies: vec![],
        waiting_on_dependencies: false,
        final_disposition: None,
    };
    let assignment = AssignmentRecord {
        id: Uuid::new_v4().to_string(),
        project_id,
        run_id,
        task_id,
        assignee: owner,
        assigned_by: ctx.actor_id(),
        attempt: 1,
        status: AssignmentStatus::Active,
        assigned_at: Utc::now(),
        ended_reason: None,
    };
    let scope = EntityScope {
        project_id: Some(project_id.0.to_string()),
        run_id: Some(run_id.0.to_string()),
        task_id: Some(task_id.0.to_string()),
        owner_id: Some(owner.0.to_string()),
    };
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.seed.assignment",
        idempotency_key: &IdempotencyKey("seed-assignment".into()),
        request: &task,
        preconditions: vec![],
        result: &task,
        changes: vec![
            StateChange::insert(
                EntityKind::Task,
                task_id.0.to_string(),
                scope.clone(),
                &task_record,
            )
            .unwrap(),
            StateChange::insert(
                EntityKind::Assignment,
                assignment.id.clone(),
                scope,
                &assignment,
            )
            .unwrap(),
        ],
        event: NewEvent::from_context(
            ctx,
            "supervisor",
            EventScope {
                project_id: Some(project_id),
                run_id: Some(run_id),
                task_id: Some(task_id),
            },
            "task.assigned",
            json!({"task_id":task_id,"owner":owner}),
        ),
    })
    .unwrap();
    task_id
}
fn seed_expired_human_request(db: &Database, ctx: &SessionContext, project_id: ProjectId) {
    let now = Utc::now();
    let decision = HumanDecision {
        id: HumanRequestId::new(),
        project_id,
        run_id: None,
        task_id: None,
        gate_key: None,
        question: "expired prompt".into(),
        context: "fixture".into(),
        choices: vec!["approve".into(), "reject".into()],
        requested_by: ctx.actor_id(),
        created_at: now - chrono::Duration::minutes(2),
        expires_at: Some(now - chrono::Duration::minutes(1)),
        on_expiry: Some(HumanExpiryAction::Expire),
        status: HumanStatus::Pending,
        resolution: None,
    };
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.seed.human",
        idempotency_key: &IdempotencyKey("seed-expired-human".into()),
        request: &decision,
        preconditions: vec![],
        result: &decision,
        changes: vec![StateChange::insert(
            EntityKind::HumanRequest,
            decision.id.0.to_string(),
            EntityScope {
                project_id: Some(project_id.0.to_string()),
                ..Default::default()
            },
            &decision,
        )
        .unwrap()],
        event: NewEvent::from_context(
            ctx,
            "supervisor",
            EventScope {
                project_id: Some(project_id),
                run_id: None,
                task_id: None,
            },
            "human.requested",
            json!({"request_id":decision.id}),
        ),
    })
    .unwrap();
}
async fn assert_ready(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let socket = path.join("coordinator.sock");
    while Instant::now() < deadline {
        if tokio::net::UnixStream::connect(&socket).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("coordinator socket did not become connectable");
}
#[cfg(unix)]
fn assert_private_state(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(path.join("coordinator.sock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(path.join("coordinator.sock.lock"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_stdio_clients_share_authenticated_durable_coordinator() {
    let temp = TempDir::new();
    let state_dir = temp.path().join("state");
    std::fs::create_dir(&state_dir).unwrap();
    let db_path = state_dir.join("tesselate.sqlite3");
    let db = Database::open(&db_path).unwrap();
    let sessions = Arc::new(SessionManager::new(db.clone()));
    let bootstrap = sessions.prepare_bootstrap().unwrap();
    let supervisor = sessions
        .bootstrap_supervisor(&bootstrap, "supervisor", IdempotencyKey("bootstrap".into()))
        .unwrap();
    let supervisor_token = supervisor.secret.unwrap();
    let supervisor_ctx = sessions.authenticate(&supervisor_token).unwrap();
    let coder = sessions
        .issue_agent(
            &supervisor_ctx,
            "coder",
            AgentRole::Coder,
            IdempotencyKey("issue-coder".into()),
        )
        .unwrap();
    let coder_token = coder.secret.unwrap();
    let project = Project {
        id: ProjectId::new(),
        name: "shared-project".into(),
    };
    db.commit_mutation(MutationInput {
        context: &supervisor_ctx,
        operation: "test.seed.project",
        idempotency_key: &IdempotencyKey("project".into()),
        request: &project,
        preconditions: vec![],
        result: &project,
        changes: vec![StateChange::insert(
            EntityKind::Project,
            project.id.0.to_string(),
            EntityScope {
                project_id: Some(project.id.0.to_string()),
                ..Default::default()
            },
            &project,
        )
        .unwrap()],
        event: NewEvent::from_context(
            &supervisor_ctx,
            "supervisor",
            EventScope {
                project_id: Some(project.id),
                run_id: None,
                task_id: None,
            },
            "project.registered",
            json!({"project_id":project.id}),
        ),
    })
    .unwrap();
    seed_expired_human_request(&db, &supervisor_ctx, project.id);

    let server = tokio::spawn(serve_coordinator(
        state_dir.clone(),
        Arc::new(coordination(db.clone())),
        sessions.clone(),
    ));
    assert_ready(&state_dir).await;
    #[cfg(unix)]
    assert_private_state(&state_dir);
    let socket = state_dir
        .join("coordinator.sock")
        .to_string_lossy()
        .into_owned();
    let duplicate_server = tokio::time::timeout(
        Duration::from_secs(1),
        serve_coordinator(
            state_dir.clone(),
            Arc::new(coordination(db.clone())),
            sessions.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(duplicate_server.kind(), std::io::ErrorKind::WouldBlock);
    let mut client_a = StdioClient::start(&socket, &supervisor_token);
    let mut client_b = StdioClient::start(&socket, &coder_token);
    for request_id in ["poll-human-a", "poll-human-b"] {
        let poll = client_a.tool(
            "tesselate.v1.human.list",
            args(
                json!({"project_id":project.id,"page":{"limit":20,"after":null}}),
                None,
                request_id,
            ),
            if request_id.ends_with('a') { 2 } else { 3 },
        );
        assert_eq!(
            poll["result"]["structuredContent"]["outcome"]["status"],
            "success"
        );
    }
    let polled_events = client_a.tool(
        "tesselate.v1.timeline.read",
        args(json!({"scope":{"project_id":project.id,"run_id":null,"task_id":null},"page":{"limit":100,"after":null}}), None, "poll-human-events"),
        4,
    )["result"]["structuredContent"]["outcome"]["data"]["items"].as_array().unwrap().clone();
    assert_eq!(
        polled_events
            .iter()
            .filter(|event| event["event_type"] == "human.expired")
            .count(),
        1
    );
    let missing_key = client_a.tool(
        "tesselate.v1.runs.create",
        args(
            json!({"project_id":project.id,"goal":"missing idempotency"}),
            None,
            "missing-key",
        ),
        5,
    );
    assert_eq!(
        missing_key["result"]["structuredContent"]["outcome"]["data"]["code"],
        "invalid_request"
    );
    let create_args = args(
        json!({"project_id":project.id,"goal":"shared run"}),
        Some("run-1"),
        "request-run",
    );
    let forbidden_args = args(
        json!({"project_id":project.id,"goal":"wrong role"}),
        Some("run-2"),
        "request-forbidden",
    );
    let (created, forbidden) = std::thread::scope(|scope| {
        let supervisor_client = &mut client_a;
        let coder_client = &mut client_b;
        let create =
            scope.spawn(move || supervisor_client.tool("tesselate.v1.runs.create", create_args, 2));
        let reject =
            scope.spawn(move || coder_client.tool("tesselate.v1.runs.create", forbidden_args, 2));
        (create.join().unwrap(), reject.join().unwrap())
    });
    assert_eq!(
        created["result"]["structuredContent"]["outcome"]["status"],
        "success"
    );
    let run: Run = serde_json::from_value(
        created["result"]["structuredContent"]["outcome"]["data"]["value"].clone(),
    )
    .unwrap();
    let replay = client_a.tool(
        "tesselate.v1.runs.create",
        args(
            json!({"project_id":project.id,"goal":"shared run"}),
            Some("run-1"),
            "request-run-retry",
        ),
        3,
    );
    assert_eq!(
        replay["result"]["structuredContent"]["outcome"]["data"]["replayed"],
        true
    );
    assert_eq!(
        replay["result"]["structuredContent"]["outcome"]["data"]["event_id"],
        created["result"]["structuredContent"]["outcome"]["data"]["event_id"]
    );

    assert_eq!(
        forbidden["result"]["structuredContent"]["outcome"]["data"]["code"],
        "forbidden"
    );
    let forged = client_b.tool("tesselate.v1.runs.create", args(json!({"project_id":project.id,"goal":"forged role","role":"supervisor","actor_id":supervisor_ctx.actor_id()}), Some("run-3"), "request-forged"), 3);
    assert_eq!(
        forged["result"]["structuredContent"]["outcome"]["data"]["code"],
        "invalid_request"
    );
    let runs = client_b.tool(
        "tesselate.v1.runs.list",
        args(
            json!({"project_id":project.id,"page":{"limit":20,"after":null}}),
            None,
            "request-runs",
        ),
        3,
    );
    assert_eq!(
        runs["result"]["structuredContent"]["outcome"]["data"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let coder_ctx = sessions.authenticate(&coder_token).unwrap();
    let task_id = seed_assignment(
        &db,
        &supervisor_ctx,
        project.id,
        run.id,
        coder_ctx.actor_id(),
    );
    let sent = client_b.tool("tesselate.v1.messages.send", args(json!({"project_id":project.id,"run_id":run.id,"task_id":task_id,"body":"resume after reconnect"}), Some("message-1"), "request-message"), 4);
    assert_eq!(
        sent["result"]["structuredContent"]["outcome"]["status"],
        "success"
    );

    let mut bad = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let wire = json!({"schema_version":1,"request_id":"bad-token","tool":"tesselate.v1.runs.create","bearer_credential":"not-a-credential","arguments":args(json!({"project_id":project.id,"goal":"no"}),Some("x"),"bad-token")});
    bad.write_all(format!("{wire}\n").as_bytes()).await.unwrap();
    let mut line = Vec::new();
    let mut bad_reader = tokio::io::BufReader::new(bad);
    bad_reader.read_until(b'\n', &mut line).await.unwrap();
    let unauthenticated: Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(
        unauthenticated["outcome"]["data"]["code"],
        "unauthenticated"
    );

    server.abort();
    let _ = server.await;
    drop(sessions);
    drop(db);
    let restarted_db = Database::open(&db_path).unwrap();
    let restarted_sessions = Arc::new(SessionManager::new(restarted_db.clone()));
    let restarted_server = tokio::spawn(serve_coordinator(
        state_dir.clone(),
        Arc::new(coordination(restarted_db)),
        restarted_sessions,
    ));
    assert_ready(&state_dir).await;
    let after_restart = client_b.tool("tesselate.v1.timeline.read", args(json!({"scope":{"project_id":project.id,"run_id":null,"task_id":null},"page":{"limit":100,"after":null}}), None, "request-events"), 4);
    let events = after_restart["result"]["structuredContent"]["outcome"]["data"]["items"]
        .as_array()
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event["event_type"] == "run.created"));
    let tasks = client_b.tool(
        "tesselate.v1.tasks.list",
        args(
            json!({"run_id":run.id,"page":{"limit":50,"after":null}}),
            None,
            "request-tasks",
        ),
        5,
    );
    let retained_task = &tasks["result"]["structuredContent"]["outcome"]["data"]["items"][0];
    assert_eq!(retained_task["id"], json!(task_id));
    assert_eq!(retained_task["owner"], json!(coder_ctx.actor_id()));
    let messages = client_b.tool(
        "tesselate.v1.messages.read",
        args(
            json!({"project_id":project.id,"task_id":task_id,"page":{"limit":50,"after":null}}),
            None,
            "request-messages",
        ),
        6,
    );
    assert_eq!(
        messages["result"]["structuredContent"]["outcome"]["data"]["items"][0]["body"],
        "resume after reconnect"
    );
    assert_eq!(run.status, RunStatus::Active);
    restarted_server.abort();
    let _ = restarted_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdio_tool_schema_lists_every_versioned_tool() {
    let temp = TempDir::new();
    let state_dir = temp.path().join("state");
    std::fs::create_dir(&state_dir).unwrap();
    let db = Database::open(state_dir.join("test.db")).unwrap();
    let sessions = Arc::new(SessionManager::new(db.clone()));
    let secret = sessions.prepare_bootstrap().unwrap();
    let credential = sessions
        .bootstrap_supervisor(&secret, "sup", IdempotencyKey("boot".into()))
        .unwrap()
        .secret
        .unwrap();
    let server = tokio::spawn(serve_coordinator(
        state_dir.clone(),
        Arc::new(coordination(db)),
        sessions,
    ));
    assert_ready(&state_dir).await;
    let socket = state_dir
        .join("coordinator.sock")
        .to_string_lossy()
        .into_owned();
    let mut client = StdioClient::start(&socket, &credential);
    let response =
        client.request(json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}));
    let tools = response["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), tesselate::mcp::TOOLS.len());
    let create_task = tools
        .iter()
        .find(|tool| tool["name"] == "tesselate.v1.tasks.create")
        .unwrap();
    assert_eq!(
        create_task["inputSchema"]["properties"]["params"]["required"],
        json!(["run_id", "title", "goal"])
    );
    assert_eq!(
        create_task["inputSchema"]["properties"]["params"]["additionalProperties"],
        false
    );
    let list_runs = tools
        .iter()
        .find(|tool| tool["name"] == "tesselate.v1.runs.list")
        .unwrap();
    assert_eq!(
        list_runs["inputSchema"]["properties"]["params"]["properties"]["page"]["properties"]
            ["after"]["anyOf"][1]["type"],
        "null"
    );
    server.abort();
    let _ = server.await;
}
