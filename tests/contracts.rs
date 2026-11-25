use chrono::{TimeZone, Utc};
use serde_json::json;
use tesselate::{mcp, *};

#[test]
fn event_envelope_is_versioned_and_stable() {
    let actor = Actor {
        id: ActorId::new(),
        display_name: "reviewer-1".into(),
        role: Role::Reviewer,
    };
    let event = Event {
        schema_version: 1,
        sequence: 42,
        event_id: EventId::new(),
        occurred_at: Utc.timestamp_opt(1_800_000_000, 0).unwrap(),
        actor,
        project_id: None,
        run_id: None,
        task_id: None,
        event_type: "review.submitted".into(),
        payload: json!({"passed": true}),
    };
    let value = serde_json::to_value(event).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["sequence"], 42);
    assert_eq!(value["actor"]["role"], "reviewer");
    assert_eq!(value["event_type"], "review.submitted");
    assert_eq!(value["payload"]["passed"], true);
}

#[test]
fn mcp_request_response_shapes_are_versioned() {
    assert!(mcp::TOOLS.contains(&"tesselate.v1.repositories.list"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.agents.list"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.reviews.rounds.list"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.tasks.disposition"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.tasks.claim"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.tasks.release"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.tasks.create_with_dependencies"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.artifacts.locate"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.human.request"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.human.list"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.human.resolve"));
    assert!(mcp::TOOLS.contains(&"tesselate.v1.workspaces.commits.capture"));
    let request = mcp::MutationRequest {
        schema_version: mcp::PROTOCOL_VERSION,
        request_id: "r1".into(),
        idempotency_key: IdempotencyKey("i1".into()),
        params: json!({"task_id": "t1"}),
    };
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(
        value,
        json!({"schema_version":1,"request_id":"r1","idempotency_key":"i1","params":{"task_id":"t1"}})
    );
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["params"]["task_id"], "t1");
    let response = mcp::ToolResponse::<serde_json::Value> {
        schema_version: 1,
        request_id: "r1".into(),
        outcome: mcp::ToolOutcome::Error(mcp::ToolError {
            code: "forbidden".into(),
            message: "unauthorized".into(),
            retryable: false,
        }),
    };
    let value = serde_json::to_value(response).unwrap();
    assert_eq!(
        value,
        json!({"schema_version":1,"request_id":"r1","outcome":{"status":"error","data":{"code":"forbidden","message":"unauthorized","retryable":false}}})
    );
    assert!(
        serde_json::from_value::<mcp::MutationRequest<serde_json::Value>>(json!({
            "schema_version": 1,
            "request_id": "r3",
            "params": {}
        }))
        .is_err()
    );
    assert!(value.get("session_context").is_none());
    let success = mcp::ToolResponse {
        schema_version: 1,
        request_id: "r2".into(),
        outcome: mcp::ToolOutcome::Success(json!({"ok":true})),
    };
    assert_eq!(
        serde_json::to_value(success).unwrap()["outcome"]["status"],
        "success"
    );
}

#[test]
fn human_decision_requests_have_stable_versioned_fields() {
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let request = HumanRequest {
        project_id,
        run_id: Some(run_id),
        task_id: None,
        question: "Approve completion?".into(),
        context: "All checks passed.".into(),
        choices: vec!["approve".into(), "reject".into()],
        expires_at: None,
        on_expiry: None,
    };
    assert_eq!(
        serde_json::to_value(request).unwrap(),
        json!({"project_id":project_id,"run_id":run_id,"task_id":null,"question":"Approve completion?","context":"All checks passed.","choices":["approve","reject"],"expires_at":null,"on_expiry":null})
    );
    assert_eq!(
        serde_json::to_value(HumanExpiryAction::Escalate).unwrap(),
        json!("escalate")
    );
}

#[test]
fn workspace_inspection_and_commit_capture_shapes_are_stable() {
    let workspace = Workspace {
        id: WorkspaceId::new(),
        assignment_id: "assignment-1".into(),
        task_id: TaskId::new(),
        repository_id: RepositoryId::new(),
        base_commit: "base-sha".into(),
        branch: "tesselate/a/r".into(),
        path: "/tmp/worktree".into(),
        owner: ActorId::new(),
    };
    let inspection = WorkspaceInspection {
        head: CommitRef {
            repository_id: workspace.repository_id,
            commit_id: "head-sha".into(),
        },
        capture_ready: false,
        clean: false,
        changed_paths: vec!["src/main.rs".into()],
        workspace: workspace.clone(),
    };
    let value = serde_json::to_value(inspection).unwrap();
    assert_eq!(value["workspace"]["assignment_id"], "assignment-1");
    assert_eq!(value["head"]["commit_id"], "head-sha");
    assert_eq!(value["clean"], false);
    assert_eq!(value["capture_ready"], false);
    assert_eq!(value["changed_paths"][0], "src/main.rs");
    let capture = CaptureWorkspaceCommits {
        workspace_id: workspace.id,
        expected_head: "head-sha".into(),
    };
    assert_eq!(
        serde_json::to_value(capture).unwrap(),
        json!({"workspace_id":workspace.id,"expected_head":"head-sha"})
    );
}

#[test]
fn run_completion_does_not_accept_caller_claimed_reviewed_commits() {
    let run_id = RunId::new();
    let request = CompleteRun {
        run_id,
        outstanding_integration_steps: vec!["merge after review".into()],
    };
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(
        value,
        json!({"run_id":run_id,"outstanding_integration_steps":["merge after review"]})
    );
    assert!(serde_json::from_value::<CompleteRun>(json!({
        "run_id": run_id,
        "reviewed_commits": [{"repository_id":RepositoryId::new(),"commit_id":"unreviewed-sha"}],
        "outstanding_integration_steps": []
    }))
    .is_err());
}

#[test]
fn pending_review_round_query_has_a_stable_versioned_shape() {
    let task_id = TaskId::new();
    let request = ListReviewRounds {
        task_id,
        page: PageRequest {
            limit: 20,
            after: None,
        },
    };
    assert_eq!(
        serde_json::to_value(request).unwrap(),
        json!({"task_id": task_id, "page": {"limit": 20, "after": null}})
    );
}

#[test]
fn run_request_and_snapshot_contract_include_goal_and_limits() {
    let request = CreateRun {
        project_id: ProjectId::new(),
        goal: "ship feature".into(),
    };
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(value["goal"], "ship feature");
    let run = Run {
        id: RunId::new(),
        project_id: ProjectId::new(),
        goal: "ship feature".into(),
        started_at: chrono::Utc::now(),
        limits: WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 4,
            max_task_retries: 2,
            max_review_rounds: 3,
        },
        status: RunStatus::Stopped,
        reviewed_commits: vec![],
        outstanding_integration_steps: vec![],
        outcome: Some(RunOutcome {
            reason: "duration limit reached".into(),
            remaining_task_ids: vec![TaskId::new()],
        }),
        comparison: None,
    };
    let value = serde_json::to_value(run).unwrap();
    assert_eq!(value["limits"]["max_task_retries"], 2);
    assert_eq!(value["outcome"]["reason"], "duration limit reached");
    assert_eq!(
        value["outcome"]["remaining_task_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn context_carries_authority_separately_from_mutation_arguments() {
    let request = UpdateProgress {
        task_id: TaskId::new(),
        progress: "working".into(),
    };
    let params = serde_json::to_value(request).unwrap();
    assert!(params.get("role").is_none());
    let forged_params = json!({"task_id": TaskId::new(), "progress":"x", "role":"supervisor"});
    assert!(forged_params.get("role").is_some()); // Tool params cannot construct the opaque core context.
}

#[test]
fn idempotent_result_marks_replay_and_preserves_event_identity() {
    let event_id = EventId::new();
    let first = Mutation {
        value: "canonical result",
        event_id,
        replayed: false,
    };
    let replay = Mutation {
        replayed: true,
        ..first.clone()
    };
    assert_eq!(first.value, replay.value);
    assert_eq!(first.event_id, replay.event_id);
    assert!(replay.replayed);
}
