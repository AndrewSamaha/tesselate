//! SQLite storage and transaction boundary. Coordination decides whether a change is allowed.

use crate::core::{
    Actor, Event, EventCursor, EventId, EventScope, IdempotencyKey, Message, Mutation, Page,
    PageRequest, Role, RunId, SessionContext, TaskId,
};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use thiserror::Error;
use uuid::Uuid;

const SCHEMA_VERSION: i64 = 2;
const MAX_PAGE_SIZE: u32 = 500;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("idempotency key was reused with a different request")]
    IdempotencyConflict,
    #[error("state precondition failed for {0}")]
    StateConflict(String),
    #[error("invalid persistence request: {0}")]
    Invalid(String),
    #[error("database schema version {found} is newer than supported version {supported}")]
    NewerSchema { found: i64, supported: i64 },
    #[error("database connection lock is poisoned")]
    Poisoned,
    #[error("stored event is invalid: {0}")]
    InvalidEvent(String),
}

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, Copy, Hash, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Project,
    Repository,
    Run,
    Task,
    Actor,
    Session,
    Assignment,
    AssignmentLease,
    Credential,
    Message,
    Workspace,
    WorkspaceCommit,
    ReviewRound,
    Review,
    ReviewFinding,
    FindingDisposition,
    HumanRequest,
    GitOperation,
    Artifact,
}

impl EntityKind {
    fn table(self) -> &'static str {
        match self {
            Self::Project => "projects",
            Self::Repository => "repositories",
            Self::Run => "runs",
            Self::Task => "tasks",
            Self::Actor => "actors",
            Self::Session => "sessions",
            Self::Assignment => "assignments",
            Self::AssignmentLease => "assignment_leases",
            Self::Credential => "credentials",
            Self::Message => "messages",
            Self::Workspace => "workspaces",
            Self::WorkspaceCommit => "workspace_commits",
            Self::ReviewRound => "review_rounds",
            Self::Review => "reviews",
            Self::ReviewFinding => "review_findings",
            Self::FindingDisposition => "finding_dispositions",
            Self::HumanRequest => "human_requests",
            Self::GitOperation => "git_operations",
            Self::Artifact => "artifact_refs",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityScope {
    pub project_id: Option<String>,
    pub run_id: Option<String>,
    pub task_id: Option<String>,
    pub owner_id: Option<String>,
}

/// Insert when `expected_revision` is `None`; update only the matching revision otherwise.
/// The revision check and write occur inside the event/idempotency transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateChange {
    pub kind: EntityKind,
    pub id: String,
    pub scope: EntityScope,
    pub payload: Value,
    pub expected_revision: Option<i64>,
    pub dependencies: Option<Vec<TaskId>>,
}

/// Compare-only durable state guard checked inside the mutation transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatePrecondition {
    pub kind: EntityKind,
    pub id: String,
    pub expected_revision: i64,
}

impl StateChange {
    pub fn insert<T: Serialize>(
        kind: EntityKind,
        id: impl Into<String>,
        scope: EntityScope,
        value: &T,
    ) -> Result<Self, PersistenceError> {
        Ok(Self {
            kind,
            id: id.into(),
            scope,
            payload: serde_json::to_value(value)?,
            expected_revision: None,
            dependencies: None,
        })
    }

    pub fn update<T: Serialize>(
        kind: EntityKind,
        id: impl Into<String>,
        scope: EntityScope,
        expected_revision: i64,
        value: &T,
    ) -> Result<Self, PersistenceError> {
        Ok(Self {
            kind,
            id: id.into(),
            scope,
            payload: serde_json::to_value(value)?,
            expected_revision: Some(expected_revision),
            dependencies: None,
        })
    }

    pub fn with_dependencies(mut self, dependencies: Vec<TaskId>) -> Self {
        self.dependencies = Some(dependencies);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewEvent {
    pub event_id: EventId,
    pub occurred_at: DateTime<Utc>,
    pub actor: Actor,
    pub project_id: Option<crate::core::ProjectId>,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub event_type: String,
    pub payload: Value,
}

impl NewEvent {
    pub fn from_context(
        ctx: &SessionContext,
        actor_name: impl Into<String>,
        scope: EventScope,
        event_type: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: EventId::new(),
            occurred_at: Utc::now(),
            actor: Actor {
                id: ctx.actor_id(),
                display_name: actor_name.into(),
                role: ctx.role(),
            },
            project_id: scope.project_id,
            run_id: scope.run_id,
            task_id: scope.task_id,
            event_type: event_type.into(),
            payload,
        }
    }
}

#[derive(Debug)]
pub struct MutationInput<'a, Q, R> {
    pub context: &'a SessionContext,
    pub operation: &'a str,
    pub idempotency_key: &'a IdempotencyKey,
    pub request: &'a Q,
    pub preconditions: Vec<StatePrecondition>,
    pub result: &'a R,
    pub changes: Vec<StateChange>,
    pub event: NewEvent,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, PersistenceError> {
        self.connection
            .lock()
            .map_err(|_| PersistenceError::Poisoned)
    }

    /// Look up a committed result before applying domain validation to a retry.
    /// `commit_mutation` remains the transactional arbiter when no row exists yet.
    pub fn prior_mutation<Q, R>(
        &self,
        context: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
    ) -> Result<Option<Mutation<R>>, PersistenceError>
    where
        Q: Serialize,
        R: DeserializeOwned,
    {
        let request_json = canonical_json(request)?;
        let connection = self.connection()?;
        let stored = connection.query_row(
            "SELECT request_json,result_json,event_id FROM idempotency_records WHERE session_id=?1 AND operation=?2 AND idempotency_key=?3",
            params![context.session_id().0.to_string(), operation, key.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
        ).optional()?;
        let Some((prior_request, result_json, event_id)) = stored else {
            return Ok(None);
        };
        if prior_request != request_json {
            return Err(PersistenceError::IdempotencyConflict);
        }
        Ok(Some(Mutation {
            value: serde_json::from_str(&result_json)?,
            event_id: parse_event_id(&event_id)?,
            replayed: true,
        }))
    }

    pub fn commit_mutation<Q, R>(
        &self,
        input: MutationInput<'_, Q, R>,
    ) -> Result<Mutation<R>, PersistenceError>
    where
        Q: Serialize,
        R: Serialize + DeserializeOwned,
    {
        validate_mutation(&input)?;
        let request_json = canonical_json(input.request)?;
        let result_json = serde_json::to_string(input.result)?;
        let session_id = input.context.session_id().0.to_string();
        let key = input.idempotency_key.0.as_str();
        let operation = input.operation;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let replay = transaction.query_row(
            "SELECT request_json, result_json, event_id FROM idempotency_records WHERE session_id=?1 AND operation=?2 AND idempotency_key=?3",
            params![session_id, operation, key],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
        ).optional()?;
        if let Some((prior_request, prior_result, event_id)) = replay {
            if prior_request != request_json {
                return Err(PersistenceError::IdempotencyConflict);
            }
            let value = serde_json::from_str(&prior_result)?;
            transaction.commit()?;
            return Ok(Mutation {
                value,
                event_id: parse_event_id(&event_id)?,
                replayed: true,
            });
        }

        for precondition in &input.preconditions {
            check_state_precondition(&transaction, precondition)?;
        }
        for change in &input.changes {
            check_precondition(&transaction, change)?;
        }
        for change in &input.changes {
            apply_change(&transaction, change)?;
        }
        let event_id = input.event.event_id;
        append_event(&transaction, &input.event)?;
        for change in &input.changes {
            transaction.execute(
                "INSERT INTO entity_events(entity_kind,entity_id,event_id) VALUES (?1,?2,?3)",
                params![change.kind.table(), change.id, event_id.0.to_string()],
            )?;
        }
        transaction.execute(
            "INSERT INTO idempotency_records(session_id, operation, idempotency_key, request_json, result_json, event_id) VALUES (?1,?2,?3,?4,?5,?6)",
            params![session_id, operation, key, request_json, result_json, event_id.0.to_string()],
        )?;
        transaction.commit()?;
        Ok(Mutation {
            value: serde_json::from_str(&result_json)?,
            event_id,
            replayed: false,
        })
    }

    pub fn events_after(
        &self,
        scope: EventScope,
        cursor: Option<&EventCursor>,
        limit: u32,
    ) -> Result<Page<Event>, PersistenceError> {
        validate_limit(limit)?;
        let connection = self.connection()?;
        let after = resolve_cursor(&connection, cursor)?;
        let mut statement = connection.prepare(
            "SELECT event_json FROM events WHERE sequence>?1 AND (?2 IS NULL OR project_id=?2) AND (?3 IS NULL OR run_id=?3) AND (?4 IS NULL OR task_id=?4) ORDER BY sequence ASC LIMIT ?5"
        )?;
        let rows = statement.query_map(
            params![
                after,
                scope.project_id.map(|id| id.0.to_string()),
                scope.run_id.map(|id| id.0.to_string()),
                scope.task_id.map(|id| id.0.to_string()),
                limit + 1
            ],
            |row| row.get::<_, String>(0),
        )?;
        let mut encoded = rows.collect::<Result<Vec<_>, _>>()?;
        encoded.truncate(limit as usize);
        let items = encoded
            .into_iter()
            .map(|json| {
                serde_json::from_str::<Event>(&json)
                    .map_err(|e| PersistenceError::InvalidEvent(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = items
            .last()
            .map(|event| issue_cursor(&connection, event.sequence))
            .transpose()?;
        Ok(Page { items, next_cursor })
    }

    pub fn messages_after(
        &self,
        project_id: crate::core::ProjectId,
        task_id: Option<TaskId>,
        cursor: Option<&EventCursor>,
        limit: u32,
    ) -> Result<Page<Message>, PersistenceError> {
        validate_limit(limit)?;
        let connection = self.connection()?;
        let after = resolve_cursor(&connection, cursor)?;
        let mut statement = connection.prepare("SELECT m.payload_json,e.sequence FROM messages m JOIN entity_events x ON x.entity_kind='messages' AND x.entity_id=m.id JOIN events e ON e.event_id=x.event_id WHERE e.sequence>?1 AND m.project_id=?2 AND (?3 IS NULL OR m.task_id=?3) ORDER BY e.sequence ASC LIMIT ?4")?;
        let rows = statement.query_map(
            params![
                after,
                project_id.0.to_string(),
                task_id.map(|id| id.0.to_string()),
                limit + 1
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
        )?;
        let encoded = rows.collect::<Result<Vec<_>, _>>()?;
        let items = encoded
            .iter()
            .take(limit as usize)
            .map(|(json, _)| serde_json::from_str(json).map_err(PersistenceError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = encoded
            .get(items.len().wrapping_sub(1))
            .filter(|_| !items.is_empty())
            .map(|(_, sequence)| issue_cursor(&connection, *sequence))
            .transpose()?;
        Ok(Page { items, next_cursor })
    }

    pub fn event_count(&self) -> Result<u64, PersistenceError> {
        let connection = self.connection()?;
        Ok(connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?)
    }

    pub fn foreign_keys_enabled(&self) -> Result<bool, PersistenceError> {
        let connection = self.connection()?;
        Ok(connection.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))? == 1)
    }

    pub fn journal_mode(&self) -> Result<String, PersistenceError> {
        let connection = self.connection()?;
        Ok(connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    pub fn load<T: DeserializeOwned>(
        &self,
        kind: EntityKind,
        id: &str,
    ) -> Result<Option<(T, i64)>, PersistenceError> {
        let connection = self.connection()?;
        let sql = format!(
            "SELECT payload_json, revision FROM {} WHERE id=?1",
            kind.table()
        );
        let row = connection
            .query_row(&sql, [id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;
        row.map(|(json, revision)| Ok((serde_json::from_str(&json)?, revision)))
            .transpose()
    }

    pub fn task_dependencies(&self, task_id: TaskId) -> Result<Vec<TaskId>, PersistenceError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT depends_on_task_id FROM task_dependencies WHERE task_id=?1 ORDER BY depends_on_task_id")?;
        let rows = statement.query_map([task_id.0.to_string()], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            let id = row?;
            Uuid::parse_str(&id)
                .map(TaskId)
                .map_err(|error| PersistenceError::Invalid(id + ": " + &error.to_string()))
        })
        .collect()
    }

    pub fn list_entities<T: DeserializeOwned>(
        &self,
        kind: EntityKind,
        scope: &EntityScope,
        page: PageRequest,
    ) -> Result<Page<T>, PersistenceError> {
        validate_limit(page.limit)?;
        let connection = self.connection()?;
        let after = resolve_cursor(&connection, page.after.as_ref())?;
        let table = kind.table();
        let sql = format!("SELECT payload_json,change_sequence FROM (SELECT r.payload_json,MAX(e.sequence) AS change_sequence FROM {table} r JOIN entity_events x ON x.entity_kind=?1 AND x.entity_id=r.id JOIN events e ON e.event_id=x.event_id WHERE (?2 IS NULL OR r.project_id=?2) AND (?3 IS NULL OR r.run_id=?3) AND (?4 IS NULL OR r.task_id=?4) AND (?5 IS NULL OR r.owner_id=?5) GROUP BY r.id) WHERE change_sequence>?6 ORDER BY change_sequence ASC LIMIT ?7");
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                table,
                scope.project_id.as_deref(),
                scope.run_id.as_deref(),
                scope.task_id.as_deref(),
                scope.owner_id.as_deref(),
                after,
                page.limit + 1
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
        )?;
        let encoded = rows.collect::<Result<Vec<_>, _>>()?;
        let items = encoded
            .iter()
            .take(page.limit as usize)
            .map(|(json, _)| serde_json::from_str(json).map_err(PersistenceError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = encoded
            .get(items.len().wrapping_sub(1))
            .filter(|_| !items.is_empty())
            .map(|(_, sequence)| issue_cursor(&connection, *sequence))
            .transpose()?;
        Ok(Page { items, next_cursor })
    }
}

fn validate_mutation<Q, R>(input: &MutationInput<'_, Q, R>) -> Result<(), PersistenceError> {
    if input.operation.is_empty() || input.idempotency_key.0.is_empty() {
        return Err(PersistenceError::Invalid(
            "operation and idempotency key are required".into(),
        ));
    }
    if input.event.actor.id != input.context.actor_id()
        || input.event.actor.role != input.context.role()
    {
        return Err(PersistenceError::Invalid(
            "event actor must match the authenticated context".into(),
        ));
    }
    if input.changes.is_empty() {
        return Err(PersistenceError::Invalid(
            "mutation must contain at least one state change".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for change in &input.changes {
        if change.id.is_empty() || !seen.insert((change.kind, change.id.as_str())) {
            return Err(PersistenceError::Invalid(
                "state changes must have unique nonempty entity IDs".into(),
            ));
        }
        if change.dependencies.as_ref().is_some_and(|dependencies| {
            change.kind != EntityKind::Task
                || dependencies.iter().any(|id| id.0.to_string() == change.id)
        }) {
            return Err(PersistenceError::Invalid(
                "dependency replacement is only valid for tasks and cannot be self-referential"
                    .into(),
            ));
        }
    }
    let mut guarded = std::collections::HashSet::new();
    for precondition in &input.preconditions {
        if precondition.id.is_empty()
            || precondition.expected_revision < 0
            || !guarded.insert((precondition.kind, precondition.id.as_str()))
        {
            return Err(PersistenceError::Invalid(
                "state preconditions need unique entity IDs and nonnegative revisions".into(),
            ));
        }
    }
    Ok(())
}

fn canonical_json<T: Serialize>(value: &T) -> Result<String, PersistenceError> {
    fn normalize(value: Value) -> Value {
        match value {
            Value::Array(values) => Value::Array(values.into_iter().map(normalize).collect()),
            Value::Object(object) => {
                let mut entries = object.into_iter().collect::<Vec<_>>();
                entries.sort_by(|left, right| left.0.cmp(&right.0));
                let mut normalized = serde_json::Map::new();
                for (key, value) in entries {
                    normalized.insert(key, normalize(value));
                }
                Value::Object(normalized)
            }
            other => other,
        }
    }
    Ok(serde_json::to_string(&normalize(serde_json::to_value(
        value,
    )?))?)
}

fn check_precondition(
    transaction: &Transaction<'_>,
    change: &StateChange,
) -> Result<(), PersistenceError> {
    let sql = format!("SELECT revision FROM {} WHERE id=?1", change.kind.table());
    let actual: Option<i64> = transaction
        .query_row(&sql, [&change.id], |row| row.get(0))
        .optional()?;
    match (change.expected_revision, actual) {
        (None, None) => Ok(()),
        (Some(expected), Some(actual)) if expected == actual => Ok(()),
        _ => Err(PersistenceError::StateConflict(change.id.clone())),
    }
}

fn check_state_precondition(
    transaction: &Transaction<'_>,
    precondition: &StatePrecondition,
) -> Result<(), PersistenceError> {
    let sql = format!(
        "SELECT revision FROM {} WHERE id=?1",
        precondition.kind.table()
    );
    let actual: Option<i64> = transaction
        .query_row(&sql, [&precondition.id], |row| row.get(0))
        .optional()?;
    if actual == Some(precondition.expected_revision) {
        Ok(())
    } else {
        Err(PersistenceError::StateConflict(precondition.id.clone()))
    }
}

fn apply_change(
    transaction: &Transaction<'_>,
    change: &StateChange,
) -> Result<(), PersistenceError> {
    let table = change.kind.table();
    if let Some(expected_revision) = change.expected_revision {
        let sql = format!("UPDATE {table} SET project_id=?2,run_id=?3,task_id=?4,owner_id=?5,payload_json=?6,revision=revision+1 WHERE id=?1 AND revision=?7");
        let changed = transaction.execute(
            &sql,
            params![
                change.id,
                change.scope.project_id,
                change.scope.run_id,
                change.scope.task_id,
                change.scope.owner_id,
                serde_json::to_string(&change.payload)?,
                expected_revision
            ],
        )?;
        if changed != 1 {
            return Err(PersistenceError::StateConflict(change.id.clone()));
        }
    } else {
        let sql = format!("INSERT INTO {table}(id,project_id,run_id,task_id,owner_id,payload_json,revision) VALUES (?1,?2,?3,?4,?5,?6,1)");
        transaction.execute(
            &sql,
            params![
                change.id,
                change.scope.project_id,
                change.scope.run_id,
                change.scope.task_id,
                change.scope.owner_id,
                serde_json::to_string(&change.payload)?
            ],
        )?;
    }
    if let Some(dependencies) = &change.dependencies {
        transaction.execute(
            "DELETE FROM task_dependencies WHERE task_id=?1",
            [&change.id],
        )?;
        for dependency in dependencies {
            transaction.execute(
                "INSERT INTO task_dependencies(task_id, depends_on_task_id) VALUES (?1,?2)",
                params![change.id, dependency.0.to_string()],
            )?;
        }
    }
    Ok(())
}

fn append_event(transaction: &Transaction<'_>, draft: &NewEvent) -> Result<(), PersistenceError> {
    let sequence: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence),0)+1 FROM events",
        [],
        |row| row.get(0),
    )?;
    let event = Event {
        schema_version: 1,
        sequence: sequence as u64,
        event_id: draft.event_id,
        occurred_at: draft.occurred_at,
        actor: draft.actor.clone(),
        project_id: draft.project_id,
        run_id: draft.run_id,
        task_id: draft.task_id,
        event_type: draft.event_type.clone(),
        payload: draft.payload.clone(),
    };
    transaction.execute(
        "INSERT INTO events(sequence,event_id,occurred_at,actor_id,role,project_id,run_id,task_id,event_type,payload_json,event_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![sequence, draft.event_id.0.to_string(), draft.occurred_at.to_rfc3339(), draft.actor.id.0.to_string(), role_name(draft.actor.role), draft.project_id.map(|id| id.0.to_string()), draft.run_id.map(|id| id.0.to_string()), draft.task_id.map(|id| id.0.to_string()), draft.event_type, serde_json::to_string(&draft.payload)?, serde_json::to_string(&event)?],
    )?;
    Ok(())
}

fn migrate(connection: &Connection) -> Result<(), PersistenceError> {
    let found: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if found > SCHEMA_VERSION {
        return Err(PersistenceError::NewerSchema {
            found,
            supported: SCHEMA_VERSION,
        });
    }
    if found == SCHEMA_VERSION {
        return Ok(());
    }
    if found == 0 {
        connection.execute_batch("BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
        CREATE TABLE projects(id TEXT PRIMARY KEY, project_id TEXT, run_id TEXT, task_id TEXT, owner_id TEXT, payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE actors(id TEXT PRIMARY KEY, project_id TEXT, run_id TEXT, task_id TEXT, owner_id TEXT, payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE repositories(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), run_id TEXT, task_id TEXT, owner_id TEXT, payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE runs(id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), run_id TEXT, task_id TEXT, owner_id TEXT, payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE tasks(id TEXT PRIMARY KEY, project_id TEXT, run_id TEXT NOT NULL REFERENCES runs(id), task_id TEXT, owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE task_dependencies(task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE, depends_on_task_id TEXT NOT NULL REFERENCES tasks(id), PRIMARY KEY(task_id,depends_on_task_id), CHECK(task_id<>depends_on_task_id));
        CREATE TABLE sessions(id TEXT PRIMARY KEY, project_id TEXT, run_id TEXT, task_id TEXT, owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE assignments(id TEXT PRIMARY KEY, project_id TEXT, run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE messages(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE workspaces(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE workspace_commits(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE review_rounds(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE reviews(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE review_findings(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE finding_dispositions(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE human_requests(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE git_operations(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE artifact_refs(id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), run_id TEXT REFERENCES runs(id), task_id TEXT REFERENCES tasks(id), owner_id TEXT REFERENCES actors(id), payload_json TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL UNIQUE,occurred_at TEXT NOT NULL,actor_id TEXT NOT NULL,role TEXT NOT NULL,project_id TEXT,run_id TEXT,task_id TEXT,event_type TEXT NOT NULL,payload_json TEXT NOT NULL,event_json TEXT NOT NULL);
        CREATE TRIGGER events_are_immutable_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'events are immutable'); END;
        CREATE TRIGGER events_are_immutable_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'events are immutable'); END;
        CREATE INDEX events_project_sequence ON events(project_id,sequence);
        CREATE INDEX events_run_sequence ON events(run_id,sequence);
        CREATE INDEX events_task_sequence ON events(task_id,sequence);
        CREATE TABLE event_cursors(token TEXT PRIMARY KEY,sequence INTEGER NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE entity_events(entity_kind TEXT NOT NULL,entity_id TEXT NOT NULL,event_id TEXT NOT NULL REFERENCES events(event_id),PRIMARY KEY(entity_kind,entity_id,event_id));
        CREATE TABLE idempotency_records(session_id TEXT NOT NULL,operation TEXT NOT NULL,idempotency_key TEXT NOT NULL,request_json TEXT NOT NULL,result_json TEXT NOT NULL,event_id TEXT NOT NULL REFERENCES events(event_id),PRIMARY KEY(session_id,operation,idempotency_key));
        INSERT INTO schema_migrations(version,applied_at) VALUES(1,CURRENT_TIMESTAMP);
        PRAGMA user_version=1;
        COMMIT;")?;
    }
    let current: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current == 1 {
        connection.execute_batch("BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS assignment_leases(id TEXT PRIMARY KEY,project_id TEXT REFERENCES projects(id),run_id TEXT REFERENCES runs(id),task_id TEXT REFERENCES tasks(id),owner_id TEXT REFERENCES actors(id),payload_json TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0));
        CREATE TABLE IF NOT EXISTS credentials(id TEXT PRIMARY KEY,project_id TEXT,run_id TEXT,task_id TEXT,owner_id TEXT REFERENCES actors(id),payload_json TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0));
        INSERT OR IGNORE INTO schema_migrations(version,applied_at) VALUES(2,CURRENT_TIMESTAMP);
        PRAGMA user_version=2;
        COMMIT;")?;
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), PersistenceError> {
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(PersistenceError::Invalid(format!(
            "page limit must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(())
}
fn resolve_cursor(
    connection: &Connection,
    cursor: Option<&EventCursor>,
) -> Result<u64, PersistenceError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    connection
        .query_row(
            "SELECT sequence FROM event_cursors WHERE token=?1",
            [&cursor.0],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| PersistenceError::Invalid("unknown event cursor".into()))
}
fn issue_cursor(connection: &Connection, sequence: u64) -> Result<String, PersistenceError> {
    let token = Uuid::new_v4().to_string();
    connection.execute(
        "INSERT INTO event_cursors(token,sequence,created_at) VALUES (?1,?2,?3)",
        params![token, sequence, Utc::now().to_rfc3339()],
    )?;
    Ok(token)
}
fn parse_event_id(value: &str) -> Result<EventId, PersistenceError> {
    Uuid::parse_str(value)
        .map(EventId)
        .map_err(|e| PersistenceError::InvalidEvent(e.to_string()))
}
fn role_name(role: Role) -> &'static str {
    match role {
        Role::Supervisor => "supervisor",
        Role::Coder => "coder",
        Role::Reviewer => "reviewer",
        Role::Cli => "cli",
    }
}

#[cfg(test)]
mod tests;
