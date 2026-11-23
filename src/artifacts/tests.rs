use super::*;
use crate::{
    core::{
        Actor, ActorId, EventId, IdempotencyKey, Message, Project, ProjectId, Role, Run, RunId,
        RunStatus, SessionContext, SessionId, TaskId, WorkflowLimits,
    },
    persistence::{EntityKind, EntityScope, MutationInput, NewEvent, StateChange},
};
use chrono::{TimeZone, Utc};
use serde_json::{json, Value};
use std::{fs, path::PathBuf};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("tesselate-f10-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn context() -> SessionContext {
    SessionContext::validated(SessionId::new(), ActorId::new(), Role::Supervisor)
}

fn record_event(db: &Database, ctx: &SessionContext, run_id: RunId, kind: &str, payload: Value) {
    let entity_id = Uuid::new_v4().to_string();
    let artifact = ArtifactRef {
        run_id,
        kind: kind.into(),
        path: "test fixture".into(),
    };
    let change = StateChange::insert(
        EntityKind::Artifact,
        entity_id,
        EntityScope::default(),
        &artifact,
    )
    .unwrap();
    let request = json!({"fixture": kind});
    let event = NewEvent::from_context(
        ctx,
        "A <reviewer> | one",
        EventScope {
            project_id: None,
            run_id: Some(run_id),
            task_id: None,
        },
        kind,
        payload,
    );
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "f10.test.event",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: &request,
        preconditions: Vec::new(),
        result: &artifact,
        changes: vec![change],
        event,
    })
    .unwrap();
}

fn seed_run(db: &Database, ctx: &SessionContext, run_id: RunId) -> ProjectId {
    let project_id = ProjectId::new();
    let project = Project {
        id: project_id,
        name: "fixture project".into(),
    };
    let actor = Actor {
        id: ctx.actor_id(),
        display_name: "Fixture Supervisor".into(),
        role: ctx.role(),
    };
    let run = Run {
        id: run_id,
        project_id,
        goal: "fixture run goal".into(),
        started_at: Utc::now(),
        limits: WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 2,
            max_task_retries: 2,
            max_review_rounds: 2,
        },
        status: RunStatus::Active,
        reviewed_commits: Vec::new(),
        outstanding_integration_steps: Vec::new(),
        outcome: None,
    };
    let changes = vec![
        StateChange::insert(
            EntityKind::Project,
            project_id.0.to_string(),
            EntityScope::default(),
            &project,
        )
        .unwrap(),
        StateChange::insert(
            EntityKind::Actor,
            ctx.actor_id().0.to_string(),
            EntityScope::default(),
            &actor,
        )
        .unwrap(),
        StateChange::insert(
            EntityKind::Run,
            run_id.0.to_string(),
            EntityScope {
                project_id: Some(project_id.0.to_string()),
                run_id: Some(run_id.0.to_string()),
                ..Default::default()
            },
            &run,
        )
        .unwrap(),
    ];
    let event = NewEvent::from_context(
        ctx,
        "Fixture Supervisor",
        EventScope {
            project_id: Some(project_id),
            run_id: Some(run_id),
            task_id: None,
        },
        "run.created",
        json!({"goal":run.goal}),
    );
    let request = json!({"run_id":run_id});
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "f10.test.seed_run",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: &request,
        preconditions: Vec::new(),
        result: &run,
        changes,
        event,
    })
    .unwrap();
    project_id
}

fn record_message(
    db: &Database,
    ctx: &SessionContext,
    run_id: RunId,
    project_id: ProjectId,
) -> String {
    let message = Message {
        id: crate::core::MessageId::new(),
        project_id,
        run_id: Some(run_id),
        task_id: None,
        sender: ctx.actor_id(),
        body: "A message needed by the standalone reader".into(),
        event_id: EventId::new(),
    };
    let change = StateChange::insert(
        EntityKind::Message,
        message.id.0.to_string(),
        EntityScope {
            project_id: Some(project_id.0.to_string()),
            run_id: Some(run_id.0.to_string()),
            owner_id: Some(ctx.actor_id().0.to_string()),
            ..Default::default()
        },
        &message,
    )
    .unwrap();
    let request = json!({"message_id":message.id});
    let event = NewEvent {
        event_id: message.event_id,
        occurred_at: Utc::now(),
        actor: Actor {
            id: ctx.actor_id(),
            display_name: "Fixture Supervisor".into(),
            role: ctx.role(),
        },
        project_id: Some(project_id),
        run_id: Some(run_id),
        task_id: None,
        event_type: "message.sent".into(),
        payload: json!({"message_id":message.id,"sender":message.sender}),
    };
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "f10.test.message",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request: &request,
        preconditions: Vec::new(),
        result: &message,
        changes: vec![change],
        event,
    })
    .unwrap();
    message.body
}

#[test]
fn repeat_export_is_deterministic_read_only_and_discoverable_after_restart() {
    let temp = TempDir::new();
    let database_path = temp.0.join("state.sqlite3");
    let artifact_root = temp.0.join("artifacts");
    let run_id = RunId::new();
    let ctx = context();
    let db = Database::open(&database_path).unwrap();
    let project_id = seed_run(&db, &ctx, run_id);
    record_event(
        &db,
        &ctx,
        run_id,
        "task.submitted",
        json!({"task_id":TaskId::new(),"commit_refs":[{"repository_id":Uuid::new_v4(),"commit_id":"abcdef0123456789"}]}),
    );
    let message_body = record_message(&db, &ctx, run_id, project_id);
    let original_event_count = db.event_count().unwrap();
    let exporter = RunArtifactExporter::new(db.clone(), &artifact_root);
    let first = exporter.export_run(run_id).unwrap();
    let first_jsonl = fs::read_to_string(&first.references[0].path).unwrap();
    let first_records = fs::read_to_string(&first.references[1].path).unwrap();
    let first_markdown = fs::read_to_string(&first.references[2].path).unwrap();
    fs::remove_file(&first.references[2].path).unwrap(); // Simulate interruption before the last artifact.
    let second = exporter.export_run(run_id).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first_jsonl,
        fs::read_to_string(&second.references[0].path).unwrap()
    );
    assert_eq!(
        first_records,
        fs::read_to_string(&second.references[1].path).unwrap()
    );
    assert_eq!(
        first_markdown,
        fs::read_to_string(&second.references[2].path).unwrap()
    );
    assert_eq!(original_event_count, db.event_count().unwrap());
    assert_eq!(first.event_count, 3);
    assert_eq!(first.record_count, 4);
    assert!(first_records.contains(&message_body));
    assert!(first_markdown.contains(&message_body));
    let message_record: Value = first_records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .find(|record: &Value| record["record_type"] == "message")
        .unwrap();
    assert_eq!(
        message_record["export_schema_version"],
        EXPORT_SCHEMA_VERSION
    );
    assert_eq!(message_record["record"]["body"], message_body);
    assert_eq!(
        exporter.locate_run(run_id).unwrap(),
        first.references,
        "F08 can discover the deterministic references without new history events"
    );
    drop(exporter);
    drop(db);

    let reopened = Database::open(&database_path).unwrap();
    let after_restart = RunArtifactExporter::new(reopened, &artifact_root)
        .export_run(run_id)
        .unwrap();
    assert_eq!(first, after_restart);
    assert_eq!(
        first_jsonl,
        fs::read_to_string(&after_restart.references[0].path).unwrap()
    );
    assert_eq!(
        first_records,
        fs::read_to_string(&after_restart.references[1].path).unwrap()
    );
    assert_eq!(
        first_markdown,
        fs::read_to_string(&after_restart.references[2].path).unwrap()
    );
}

#[test]
fn export_handles_incomplete_history_escaping_and_sensitive_fields() {
    let temp = TempDir::new();
    let db = Database::open(temp.0.join("state.sqlite3")).unwrap();
    let run_id = RunId::new();
    let ctx = context();
    seed_run(&db, &ctx, run_id);
    record_event(
        &db,
        &ctx,
        run_id,
        "task.progress",
        json!({
            "progress":"unsafe | <tag> `code` \\\nnext",
            "nested":{"bootstrap_secret":"never-export-this","api_token":"also-secret","safe":"kept","canonical_path":"/not-portable"},
            "password":"nope"
        }),
    );
    // Even if a future producer accidentally scopes this event to a run, credential events are
    // excluded as a whole.
    record_event(
        &db,
        &ctx,
        run_id,
        "credential.issued",
        json!({"credential":"never-export-this-either"}),
    );
    let exported = RunArtifactExporter::new(db, temp.0.join("artifacts"))
        .export_run(run_id)
        .unwrap();
    assert!(!exported.terminal_outcome_recorded);
    assert_eq!(exported.event_count, 2);
    let jsonl = fs::read_to_string(&exported.references[0].path).unwrap();
    let line = jsonl
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|line| line["event"]["event_type"] == "task.progress")
        .unwrap();
    assert_eq!(line["export_schema_version"], EXPORT_SCHEMA_VERSION);
    assert_eq!(line["event"]["schema_version"], 1);
    assert_eq!(line["event"]["payload"]["nested"]["safe"], "kept");
    assert!(jsonl.contains("task.progress"));
    assert!(!jsonl.contains("never-export-this"));
    assert!(!jsonl.contains("also-secret"));
    assert!(!jsonl.contains("nope"));
    assert!(!jsonl.contains("not-portable"));
    assert!(!jsonl.contains("credential.issued"));

    let markdown = fs::read_to_string(&exported.references[2].path).unwrap();
    assert!(markdown.contains("incomplete snapshot"));
    assert!(markdown.contains("&#124;"));
    assert!(markdown.contains("&lt;tag&gt;"));
    assert!(markdown.contains("&#96;code&#96;"));
    assert!(markdown.contains("&#92;nnext"));
    assert!(!markdown.contains("never-export-this"));
    assert!(!markdown.contains("<tag>"));
}

#[test]
fn jsonl_and_timeline_sort_events_by_sequence_and_link_related_refs() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let mut older = fixture_event(run_id, 1, "task.created", json!({"task_id":task_id}));
    let newer = fixture_event(
        run_id,
        2,
        "review.round_requested",
        json!({"round_id":Uuid::new_v4(),"commit_refs":[{"repository_id":Uuid::new_v4(),"commit_id":"1234567890abcdef"}]}),
    );
    let terminal = fixture_event(
        run_id,
        3,
        "run.completed",
        json!({"reviewed_commits":[{"repository_id":Uuid::new_v4(),"commit_id":"deadc0de12345678"}],"remaining_task_ids":[TaskId::new()]}),
    );
    older.task_id = Some(task_id);
    let actor_id = older.actor.id;
    let jsonl =
        render_events_jsonl(run_id, &[terminal.clone(), newer.clone(), older.clone()]).unwrap();
    let sequences = jsonl
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["event"]["sequence"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![1, 2, 3]);
    let markdown = render_timeline_markdown(run_id, &[terminal, newer, older]).unwrap();
    assert!(markdown.contains("terminal outcome recorded"));
    assert!(markdown.contains(&format!("#task-{}", task_id.0)));
    assert!(markdown.contains(&format!("#actor-{}", actor_id.0)));
    assert!(markdown.contains("#review-round-"));
    assert!(markdown.contains("#repository-"));
    assert!(markdown.contains("#commit-1234567890abcdef"));
}

#[test]
fn related_record_export_omits_secret_and_local_path_fields() {
    let mut record = json!({
        "id":Uuid::new_v4(),
        "api_key":"must-not-appear",
        "nested":{"secret_config":"also-hidden","canonical_path":"/private/repo","safe":"kept"}
    });
    redact_sensitive_fields(&mut record);
    let records = vec![ExportRecord {
        record_type: "test".into(),
        record,
    }];
    let exported = render_records_jsonl(&records).unwrap();
    assert!(exported.contains("kept"));
    assert!(!exported.contains("must-not-appear"));
    assert!(!exported.contains("also-hidden"));
    assert!(!exported.contains("/private/repo"));
}

#[test]
fn export_paginates_large_event_history_in_sequence_order() {
    let temp = TempDir::new();
    let db = Database::open(temp.0.join("state.sqlite3")).unwrap();
    let run_id = RunId::new();
    let ctx = context();
    seed_run(&db, &ctx, run_id);
    for index in 0..502 {
        record_event(&db, &ctx, run_id, "task.progress", json!({"step":index}));
    }
    let exported = RunArtifactExporter::new(db, temp.0.join("artifacts"))
        .export_run(run_id)
        .unwrap();
    assert_eq!(exported.event_count, 503);
    let jsonl = fs::read_to_string(&exported.references[0].path).unwrap();
    let sequences = jsonl
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["event"]["sequence"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(sequences, (1..=503).collect::<Vec<_>>());
}

#[cfg(unix)]
#[test]
fn export_refuses_a_symlinked_run_directory() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new();
    let db = Database::open(temp.0.join("state.sqlite3")).unwrap();
    let run_id = RunId::new();
    let ctx = context();
    seed_run(&db, &ctx, run_id);
    let artifact_root = temp.0.join("artifacts");
    let runs_dir = artifact_root.join("runs");
    fs::create_dir_all(&runs_dir).unwrap();
    let outside = temp.0.join("outside");
    fs::create_dir(&outside).unwrap();
    symlink(&outside, runs_dir.join(run_id.0.to_string())).unwrap();

    let result = RunArtifactExporter::new(db, &artifact_root).export_run(run_id);
    assert!(matches!(result, Err(ArtifactError::Invalid(_))));
    assert!(fs::read_dir(outside).unwrap().next().is_none());
}

#[test]
fn failed_atomic_replacement_restores_the_previous_artifact() {
    let temp = TempDir::new();
    let destination = temp.0.join("timeline.v1.md");
    fs::write(&destination, "old artifact").unwrap();
    let mut rename_calls = 0;
    let result = write_atomic_with(
        &destination,
        b"new artifact",
        &mut |from: &Path, to: &Path| {
            if from
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
            {
                rename_calls += 1;
                return Err(io::Error::new(
                    if rename_calls == 1 {
                        io::ErrorKind::AlreadyExists
                    } else {
                        io::ErrorKind::PermissionDenied
                    },
                    "injected replacement failure",
                ));
            }
            fs::rename(from, to)
        },
        |parent| sync_parent_directory(parent),
    );
    assert!(result.is_err());
    assert_eq!(fs::read_to_string(destination).unwrap(), "old artifact");
}

#[test]
fn directory_sync_failure_restores_the_previous_artifact() {
    let temp = TempDir::new();
    let destination = temp.0.join("timeline.v1.md");
    fs::write(&destination, "old artifact").unwrap();
    let mut sync_calls = 0;
    let result = write_atomic_with(
        &destination,
        b"new artifact",
        |from, to| fs::rename(from, to),
        |_| {
            sync_calls += 1;
            if sync_calls == 1 {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected directory sync failure",
                ))
            } else {
                Ok(())
            }
        },
    );
    assert!(result.is_err());
    assert_eq!(fs::read_to_string(destination).unwrap(), "old artifact");
}

#[cfg(unix)]
#[test]
fn syncing_the_parent_directory_works_on_the_supported_unix_host() {
    let temp = TempDir::new();
    sync_parent_directory(&temp.0).unwrap();
}

fn fixture_event(run_id: RunId, sequence: u64, event_type: &str, payload: Value) -> Event {
    Event {
        schema_version: 1,
        sequence,
        event_id: EventId::new(),
        occurred_at: Utc
            .timestamp_opt(1_700_000_000 + sequence as i64, 0)
            .unwrap(),
        actor: crate::core::Actor {
            id: ActorId::new(),
            display_name: "fixture".into(),
            role: Role::Reviewer,
        },
        project_id: None,
        run_id: Some(run_id),
        task_id: None,
        event_type: event_type.into(),
        payload,
    }
}
