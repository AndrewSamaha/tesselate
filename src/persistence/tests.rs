use super::*;
use crate::core::{ActorId, ProjectId, SessionId};
use serde_json::json;
use std::{fs, path::PathBuf};

struct TempDb(PathBuf);
impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("tesselate-f02-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path.join("state.sqlite3"))
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        if let Some(parent) = self.0.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }
}

fn context() -> SessionContext {
    SessionContext::validated(SessionId::new(), ActorId::new(), Role::Supervisor)
}
fn event(ctx: &SessionContext, event_type: &str, project_id: Option<ProjectId>) -> NewEvent {
    NewEvent::from_context(
        ctx,
        "test-actor",
        EventScope {
            project_id,
            run_id: None,
            task_id: None,
        },
        event_type,
        json!({"ok":true}),
    )
}
fn insert<T: Serialize + Clone>(
    db: &Database,
    ctx: &SessionContext,
    kind: EntityKind,
    id: String,
    value: &T,
    request: &Value,
    result: &Value,
) -> Result<Mutation<Value>, PersistenceError> {
    let change = StateChange::insert(kind, id, EntityScope::default(), value)?;
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.insert",
        idempotency_key: &IdempotencyKey(Uuid::new_v4().to_string()),
        request,
        result,
        change,
        event: event(ctx, "test.inserted", None),
    })
}

#[test]
fn migration_configures_wal_foreign_keys_and_rejects_newer_schema() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    assert!(db.foreign_keys_enabled().unwrap());
    assert_eq!(db.journal_mode().unwrap().to_ascii_lowercase(), "wal");
    drop(db);
    let connection = Connection::open(&temp.0).unwrap();
    connection.pragma_update(None, "user_version", 99).unwrap();
    drop(connection);
    assert!(matches!(
        Database::open(&temp.0),
        Err(PersistenceError::NewerSchema { found: 99, .. })
    ));
}

#[test]
fn state_event_and_idempotency_are_atomic_and_replay_stably() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let ctx = context();
    let project = ProjectId::new();
    let request = json!({"name":"alpha"});
    let result = json!({"id":project});
    let key = IdempotencyKey("same-call".into());
    let change = StateChange::insert(
        EntityKind::Project,
        project.0.to_string(),
        EntityScope::default(),
        &request,
    )
    .unwrap();
    let input = || MutationInput {
        context: &ctx,
        operation: "projects.create",
        idempotency_key: &key,
        request: &request,
        result: &result,
        change: change.clone(),
        event: event(&ctx, "project.created", Some(project)),
    };
    let first = db.commit_mutation(input()).unwrap();
    drop(db);
    let reopened = Database::open(&temp.0).unwrap();
    let replay = reopened.commit_mutation(input()).unwrap();
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(first.value, replay.value);
    assert_eq!(first.event_id, replay.event_id);
    assert_eq!(reopened.event_count().unwrap(), 1);
    let external = Connection::open(&temp.0).unwrap();
    assert!(external
        .execute("UPDATE events SET event_type='rewritten'", [])
        .is_err());
    assert!(external.execute("DELETE FROM events", []).is_err());
    drop(external);
    assert!(matches!(
        reopened.commit_mutation(MutationInput {
            request: &json!({"name":"different"}),
            ..input()
        }),
        Err(PersistenceError::IdempotencyConflict)
    ));
    assert_eq!(reopened.event_count().unwrap(), 1);
}

#[test]
fn failed_state_precondition_rolls_back_event_and_idempotency_record() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let ctx = context();
    let task_id = crate::core::TaskId::new();
    let request = json!({"task":"update"});
    let result = json!({"updated":true});
    let change = StateChange::update(
        EntityKind::Task,
        task_id.0.to_string(),
        EntityScope::default(),
        7,
        &result,
    )
    .unwrap();
    let outcome = db.commit_mutation(MutationInput {
        context: &ctx,
        operation: "tasks.update",
        idempotency_key: &IdempotencyKey("wrong-revision".into()),
        request: &request,
        result: &result,
        change,
        event: event(&ctx, "task.updated", None),
    });
    assert!(matches!(outcome, Err(PersistenceError::StateConflict(_))));
    assert_eq!(db.event_count().unwrap(), 0);
    assert!(db
        .load::<Value>(EntityKind::Task, &task_id.0.to_string())
        .unwrap()
        .is_none());
}

#[test]
fn dependencies_and_event_cursor_are_consistent() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let ctx = context();
    let project = ProjectId::new();
    insert(
        &db,
        &ctx,
        EntityKind::Project,
        project.0.to_string(),
        &json!({"name":"p"}),
        &json!(1),
        &json!(1),
    )
    .unwrap();
    let run_id = crate::core::RunId::new();
    let run_scope = EntityScope {
        project_id: Some(project.0.to_string()),
        ..Default::default()
    };
    let run_change = StateChange::insert(
        EntityKind::Run,
        run_id.0.to_string(),
        run_scope,
        &json!({"id":run_id}),
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: &ctx,
        operation: "runs.create",
        idempotency_key: &IdempotencyKey("run1".into()),
        request: &json!({"run_id":run_id}),
        result: &json!({"run_id":run_id}),
        change: run_change,
        event: event(&ctx, "run.created", Some(project)),
    })
    .unwrap();
    let first_task = crate::core::TaskId::new();
    let second_task = crate::core::TaskId::new();
    for (id, deps, key) in [
        (first_task, vec![], "task1"),
        (second_task, vec![first_task], "task2"),
    ] {
        let task_scope = EntityScope {
            project_id: Some(project.0.to_string()),
            run_id: Some(run_id.0.to_string()),
            ..Default::default()
        };
        let change = StateChange::insert(
            EntityKind::Task,
            id.0.to_string(),
            task_scope,
            &json!({"id":id}),
        )
        .unwrap()
        .with_dependencies(deps);
        db.commit_mutation(MutationInput {
            context: &ctx,
            operation: "tasks.create",
            idempotency_key: &IdempotencyKey(key.into()),
            request: &json!({"id":id}),
            result: &json!({"id":id}),
            change,
            event: event(&ctx, "task.created", Some(project)),
        })
        .unwrap();
    }
    let page = db
        .events_after(
            EventScope {
                project_id: Some(project),
                run_id: None,
                task_id: None,
            },
            None,
            1,
        )
        .unwrap();
    assert_eq!(page.items.len(), 1);
    let cursor = EventCursor(page.next_cursor.unwrap());
    let next = db
        .events_after(
            EventScope {
                project_id: Some(project),
                run_id: None,
                task_id: None,
            },
            Some(&cursor),
            10,
        )
        .unwrap();
    assert_eq!(next.items.len(), 2);
    assert!(next
        .items
        .windows(2)
        .all(|events| events[0].sequence < events[1].sequence));
    let task_scope = EntityScope {
        project_id: Some(project.0.to_string()),
        run_id: Some(run_id.0.to_string()),
        ..Default::default()
    };
    let tasks = db
        .list_entities::<Value>(
            EntityKind::Task,
            &task_scope,
            PageRequest {
                limit: 1,
                after: None,
            },
        )
        .unwrap();
    assert_eq!(tasks.items.len(), 1);
    let task_cursor = EventCursor(tasks.next_cursor.unwrap());
    let remaining = db
        .list_entities::<Value>(
            EntityKind::Task,
            &task_scope,
            PageRequest {
                limit: 10,
                after: Some(task_cursor),
            },
        )
        .unwrap();
    assert_eq!(remaining.items.len(), 1);
    assert!(db
        .events_after(
            EventScope {
                project_id: None,
                run_id: None,
                task_id: None
            },
            Some(&EventCursor("forged".into())),
            10
        )
        .is_err());
}

#[test]
fn message_reads_use_persistent_sequence_cursors() {
    let temp = TempDb::new();
    let ctx = context();
    let project = ProjectId::new();
    let db = Database::open(&temp.0).unwrap();
    insert(
        &db,
        &ctx,
        EntityKind::Project,
        project.0.to_string(),
        &json!({"name":"messages"}),
        &json!(1),
        &json!(1),
    )
    .unwrap();
    insert(
        &db,
        &ctx,
        EntityKind::Actor,
        ctx.actor_id().0.to_string(),
        &json!({"role":"supervisor"}),
        &json!("actor"),
        &json!("actor"),
    )
    .unwrap();
    for body in ["first", "second"] {
        let draft = event(&ctx, "message.sent", Some(project));
        let message = Message {
            id: crate::core::MessageId::new(),
            project_id: project,
            run_id: None,
            task_id: None,
            sender: ctx.actor_id(),
            body: body.into(),
            event_id: draft.event_id,
        };
        let change = StateChange::insert(
            EntityKind::Message,
            message.id.0.to_string(),
            EntityScope {
                project_id: Some(project.0.to_string()),
                ..Default::default()
            },
            &message,
        )
        .unwrap();
        db.commit_mutation(MutationInput {
            context: &ctx,
            operation: "messages.send",
            idempotency_key: &IdempotencyKey(body.into()),
            request: &message,
            result: &message,
            change,
            event: draft,
        })
        .unwrap();
    }
    let page = db.messages_after(project, None, None, 1).unwrap();
    assert_eq!(page.items[0].body, "first");
    let next = EventCursor(page.next_cursor.unwrap());
    drop(db);
    let reopened = Database::open(&temp.0).unwrap();
    let second = reopened
        .messages_after(project, None, Some(&next), 1)
        .unwrap();
    assert_eq!(second.items[0].body, "second");
    assert!(second.next_cursor.is_none());
}

#[test]
fn durable_domain_records_survive_reopen() {
    let temp = TempDb::new();
    let ctx = context();
    let project = ProjectId::new();
    let db = Database::open(&temp.0).unwrap();
    insert(
        &db,
        &ctx,
        EntityKind::Project,
        project.0.to_string(),
        &json!({"name":"reopen"}),
        &json!(1),
        &json!(1),
    )
    .unwrap();
    insert(
        &db,
        &ctx,
        EntityKind::Actor,
        ctx.actor_id().0.to_string(),
        &json!({"role":"supervisor"}),
        &json!("actor"),
        &json!("actor"),
    )
    .unwrap();
    let run = crate::core::RunId::new();
    let scope = EntityScope {
        project_id: Some(project.0.to_string()),
        ..Default::default()
    };
    let run_change = StateChange::insert(
        EntityKind::Run,
        run.0.to_string(),
        scope.clone(),
        &json!({"status":"active"}),
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: &ctx,
        operation: "runs.create",
        idempotency_key: &IdempotencyKey("durable-run".into()),
        request: &json!("run"),
        result: &json!({"run":run}),
        change: run_change,
        event: event(&ctx, "run.created", Some(project)),
    })
    .unwrap();
    let task = crate::core::TaskId::new();
    let task_scope = EntityScope {
        run_id: Some(run.0.to_string()),
        project_id: Some(project.0.to_string()),
        owner_id: Some(ctx.actor_id().0.to_string()),
        ..Default::default()
    };
    let task_change = StateChange::insert(
        EntityKind::Task,
        task.0.to_string(),
        task_scope.clone(),
        &json!({"owner":ctx.actor_id()}),
    )
    .unwrap();
    db.commit_mutation(MutationInput {
        context: &ctx,
        operation: "tasks.assign",
        idempotency_key: &IdempotencyKey("durable-task".into()),
        request: &json!("task"),
        result: &json!({"task":task}),
        change: task_change,
        event: event(&ctx, "task.assigned", Some(project)),
    })
    .unwrap();
    for (kind, id, value) in [
        (EntityKind::Session, "session", json!({"lease":"active"})),
        (
            EntityKind::Assignment,
            "assignment",
            json!({"owner":ctx.actor_id()}),
        ),
        (EntityKind::Message, "message", json!({"body":"persisted"})),
        (
            EntityKind::Workspace,
            "workspace",
            json!({"path":"/tmp/work"}),
        ),
        (EntityKind::WorkspaceCommit, "commit", json!({"sha":"abc"})),
        (EntityKind::ReviewRound, "round", json!({"round":1})),
        (EntityKind::Review, "review", json!({"passed":true})),
        (
            EntityKind::ReviewFinding,
            "finding",
            json!({"severity":"high"}),
        ),
        (
            EntityKind::FindingDisposition,
            "disposition",
            json!({"status":"fixed"}),
        ),
        (
            EntityKind::HumanRequest,
            "human",
            json!({"question":"approve?"}),
        ),
        (
            EntityKind::GitOperation,
            "git-op",
            json!({"checkpoint":"prepared"}),
        ),
        (
            EntityKind::Artifact,
            "artifact",
            json!({"path":"run.jsonl"}),
        ),
    ] {
        let state_scope = EntityScope {
            project_id: Some(project.0.to_string()),
            run_id: Some(run.0.to_string()),
            task_id: Some(task.0.to_string()),
            owner_id: Some(ctx.actor_id().0.to_string()),
        };
        let change = StateChange::insert(kind, id, state_scope, &value).unwrap();
        insert_state(&db, &ctx, change, &value, id);
    }
    let prior_events = db
        .events_after(
            EventScope {
                project_id: Some(project),
                run_id: None,
                task_id: None,
            },
            None,
            100,
        )
        .unwrap()
        .items;
    drop(db);
    let reopened = Database::open(&temp.0).unwrap();
    let (saved, _) = reopened
        .load::<Value>(EntityKind::Message, "message")
        .unwrap()
        .unwrap();
    assert_eq!(saved["body"], "persisted");
    assert_eq!(
        reopened
            .load::<Value>(EntityKind::Review, "review")
            .unwrap()
            .unwrap()
            .0["passed"],
        true
    );
    assert_eq!(
        reopened
            .load::<Value>(EntityKind::GitOperation, "git-op")
            .unwrap()
            .unwrap()
            .0["checkpoint"],
        "prepared"
    );
    assert_eq!(
        reopened
            .load::<Value>(EntityKind::Artifact, "artifact")
            .unwrap()
            .unwrap()
            .0["path"],
        "run.jsonl"
    );
    for (kind, id) in [
        (EntityKind::Session, "session"),
        (EntityKind::Assignment, "assignment"),
        (EntityKind::Workspace, "workspace"),
        (EntityKind::WorkspaceCommit, "commit"),
        (EntityKind::ReviewRound, "round"),
        (EntityKind::ReviewFinding, "finding"),
        (EntityKind::FindingDisposition, "disposition"),
        (EntityKind::HumanRequest, "human"),
    ] {
        assert!(reopened.load::<Value>(kind, id).unwrap().is_some());
    }
    let restored_events = reopened
        .events_after(
            EventScope {
                project_id: Some(project),
                run_id: None,
                task_id: None,
            },
            None,
            100,
        )
        .unwrap()
        .items;
    assert_eq!(restored_events, prior_events);
    assert!(reopened.event_count().unwrap() >= 15);
}

fn insert_state(db: &Database, ctx: &SessionContext, change: StateChange, value: &Value, id: &str) {
    let request = json!({"id":id});
    db.commit_mutation(MutationInput {
        context: ctx,
        operation: "test.persist",
        idempotency_key: &IdempotencyKey(format!("persist-{id}")),
        request: &request,
        result: value,
        change,
        event: event(ctx, "test.persisted", None),
    })
    .unwrap();
}

#[test]
fn concurrent_mutations_serialize_without_losing_events() {
    let temp = TempDb::new();
    let db = Database::open(&temp.0).unwrap();
    let mut threads = Vec::new();
    for index in 0..8 {
        let db = Database::open(&temp.0).unwrap();
        threads.push(std::thread::spawn(move || {
            let ctx = context();
            let id = ProjectId::new();
            let value = json!({"name":format!("project-{index}")});
            let change = StateChange::insert(
                EntityKind::Project,
                id.0.to_string(),
                EntityScope::default(),
                &value,
            )
            .unwrap();
            db.commit_mutation(MutationInput {
                context: &ctx,
                operation: "projects.create",
                idempotency_key: &IdempotencyKey(format!("parallel-{index}")),
                request: &value,
                result: &value,
                change,
                event: event(&ctx, "project.created", Some(id)),
            })
            .unwrap();
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(db.event_count().unwrap(), 8);
}
