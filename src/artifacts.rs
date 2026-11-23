//! Deterministic, portable exports of immutable run events (F10).

use crate::{
    core::{Actor, ArtifactRef, Event, EventCursor, EventScope, PageRequest, Project, Run, RunId},
    persistence::{Database, EntityKind, EntityScope, PersistenceError},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;
use uuid::Uuid;

pub const EXPORT_SCHEMA_VERSION: u16 = 1;
pub const EVENTS_JSONL_KIND: &str = "events_jsonl_v1";
pub const RECORDS_JSONL_KIND: &str = "records_jsonl_v1";
pub const TIMELINE_MARKDOWN_KIND: &str = "timeline_markdown_v1";
const EVENT_PAGE_SIZE: u32 = 500;

#[derive(Serialize)]
struct ExportEventLine<'a> {
    export_schema_version: u16,
    event: &'a Event,
}

#[derive(Serialize)]
struct ExportRecordLine<'a> {
    export_schema_version: u16,
    record_type: &'a str,
    record: &'a Value,
}

#[derive(Debug, Clone, PartialEq)]
struct ExportRecord {
    record_type: String,
    record: Value,
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("event history could not be read: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("artifact output failed: {0}")]
    Io(#[from] io::Error),
    #[error("run event history is invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedRunArtifacts {
    pub run_id: RunId,
    pub schema_version: u16,
    pub event_count: usize,
    pub record_count: usize,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub terminal_outcome_recorded: bool,
    pub references: Vec<ArtifactRef>,
}

/// Reads a run's immutable event history and writes three deterministic files under a validated
/// project artifact directory. It never adds events or changes workflow state.
#[derive(Clone)]
pub struct RunArtifactExporter {
    db: Database,
    artifact_root: PathBuf,
}

impl RunArtifactExporter {
    pub fn new(db: Database, artifact_root: impl AsRef<Path>) -> Self {
        Self {
            db,
            artifact_root: artifact_root.as_ref().to_owned(),
        }
    }

    /// Exports a bounded snapshot. Re-running after interruption replaces the same paths with
    /// content derived from the current event and related-record history.
    pub fn export_run(&self, run_id: RunId) -> Result<ExportedRunArtifacts, ArtifactError> {
        let events = self.read_run_events(run_id)?;
        let records = self.read_related_records(run_id, &events)?;
        let jsonl = render_events_jsonl(run_id, &events)?;
        let records_jsonl = render_records_jsonl(&records)?;
        let markdown = render_timeline_with_records(run_id, &events, &records)?;

        fs::create_dir_all(&self.artifact_root)?;
        let root = fs::canonicalize(&self.artifact_root)?;
        let runs_dir = root.join("runs");
        ensure_plain_directory(&runs_dir)?;
        let run_dir = runs_dir.join(run_id.0.to_string());
        ensure_plain_directory(&run_dir)?;

        let jsonl_path = run_dir.join("events.v1.jsonl");
        let records_path = run_dir.join("records.v1.jsonl");
        let markdown_path = run_dir.join("timeline.v1.md");
        write_atomic(&jsonl_path, jsonl.as_bytes())?;
        write_atomic(&records_path, records_jsonl.as_bytes())?;
        write_atomic(&markdown_path, markdown.as_bytes())?;

        Ok(ExportedRunArtifacts {
            run_id,
            schema_version: EXPORT_SCHEMA_VERSION,
            event_count: events.len(),
            record_count: records.len(),
            first_sequence: events.first().map(|event| event.sequence),
            last_sequence: events.last().map(|event| event.sequence),
            terminal_outcome_recorded: has_terminal_outcome(&events),
            references: vec![
                artifact_ref(run_id, EVENTS_JSONL_KIND, &jsonl_path),
                artifact_ref(run_id, RECORDS_JSONL_KIND, &records_path),
                artifact_ref(run_id, TIMELINE_MARKDOWN_KIND, &markdown_path),
            ],
        })
    }

    /// Resolves the deterministic artifact references for F08 discovery. Only existing regular
    /// files are returned; authorization remains with the calling coordination adapter.
    pub fn locate_run(&self, run_id: RunId) -> Result<Vec<ArtifactRef>, ArtifactError> {
        let canonical_root = match fs::canonicalize(&self.artifact_root) {
            Ok(root) => root,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let runs_dir = canonical_root.join("runs");
        if !is_plain_directory(&runs_dir)? {
            return Ok(Vec::new());
        }
        let canonical_dir = runs_dir.join(run_id.0.to_string());
        if !is_plain_directory(&canonical_dir)? {
            return Ok(Vec::new());
        }
        let candidates = [
            (EVENTS_JSONL_KIND, "events.v1.jsonl"),
            (RECORDS_JSONL_KIND, "records.v1.jsonl"),
            (TIMELINE_MARKDOWN_KIND, "timeline.v1.md"),
        ];
        let mut references = Vec::new();
        for (kind, name) in candidates {
            let path = canonical_dir.join(name);
            if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_file()) {
                references.push(artifact_ref(run_id, kind, &path));
            }
        }
        Ok(references)
    }

    fn read_run_events(&self, run_id: RunId) -> Result<Vec<Event>, ArtifactError> {
        let scope = EventScope {
            project_id: None,
            run_id: Some(run_id),
            task_id: None,
        };
        let mut cursor: Option<EventCursor> = None;
        let mut events = Vec::new();
        loop {
            let page = self
                .db
                .events_after(scope.clone(), cursor.as_ref(), EVENT_PAGE_SIZE)?;
            events.extend(page.items);
            match page.next_cursor {
                Some(next) if events.len() % EVENT_PAGE_SIZE as usize == 0 => {
                    cursor = Some(EventCursor(next));
                }
                _ => break,
            }
        }
        normalize_events(run_id, events)
    }

    fn read_related_records(
        &self,
        run_id: RunId,
        events: &[Event],
    ) -> Result<Vec<ExportRecord>, ArtifactError> {
        let scope = EntityScope {
            run_id: Some(run_id.0.to_string()),
            ..Default::default()
        };
        let mut records = Vec::new();
        if let Some((run, _)) = self
            .db
            .load::<Run>(EntityKind::Run, &run_id.0.to_string())?
        {
            let project_id = run.project_id;
            records.push(export_record("run", run)?);
            if let Some((project, _)) = self
                .db
                .load::<Project>(EntityKind::Project, &project_id.0.to_string())?
            {
                records.push(export_record("project", project)?);
            }
            let project_scope = EntityScope {
                project_id: Some(project_id.0.to_string()),
                ..Default::default()
            };
            records.extend(
                self.list_records(EntityKind::Repository, &project_scope)?
                    .into_iter()
                    .map(|record| ExportRecord {
                        record_type: "repository".into(),
                        record,
                    }),
            );
        }
        let mut actor_ids = events
            .iter()
            .map(|event| event.actor.id)
            .collect::<Vec<_>>();
        actor_ids.sort_by_key(|actor_id| actor_id.0);
        actor_ids.dedup();
        for actor_id in actor_ids {
            if let Some((actor, _)) = self
                .db
                .load::<Actor>(EntityKind::Actor, &actor_id.0.to_string())?
            {
                records.push(export_record("actor", actor)?);
            }
        }
        let kinds = [
            (EntityKind::Task, "task"),
            (EntityKind::Assignment, "assignment"),
            (EntityKind::Message, "message"),
            (EntityKind::Workspace, "workspace"),
            (EntityKind::WorkspaceCommit, "workspace_commit"),
            (EntityKind::ReviewRound, "review_round"),
            (EntityKind::Review, "review"),
            (EntityKind::ReviewFinding, "review_finding"),
            (EntityKind::FindingDisposition, "finding_disposition"),
            (EntityKind::HumanRequest, "human_decision"),
        ];
        for (kind, record_type) in kinds {
            records.extend(self.list_records(kind, &scope)?.into_iter().map(|record| {
                ExportRecord {
                    record_type: record_type.into(),
                    record,
                }
            }));
        }
        for record in &mut records {
            redact_sensitive_fields(&mut record.record);
        }
        records.sort_by(|left, right| {
            left.record_type
                .cmp(&right.record_type)
                .then_with(|| canonical_value(&left.record).cmp(&canonical_value(&right.record)))
        });
        Ok(records)
    }

    fn list_records(
        &self,
        kind: EntityKind,
        scope: &EntityScope,
    ) -> Result<Vec<Value>, ArtifactError> {
        let mut after = None;
        let mut records = Vec::new();
        loop {
            let page = self.db.list_entities::<Value>(
                kind,
                scope,
                PageRequest {
                    limit: EVENT_PAGE_SIZE,
                    after: after.clone(),
                },
            )?;
            records.extend(page.items);
            match page.next_cursor {
                Some(next) if records.len() % EVENT_PAGE_SIZE as usize == 0 => {
                    after = Some(EventCursor(next));
                }
                _ => break,
            }
        }
        Ok(records)
    }
}

fn export_record<T: Serialize>(
    record_type: &str,
    record: T,
) -> Result<ExportRecord, ArtifactError> {
    Ok(ExportRecord {
        record_type: record_type.into(),
        record: serde_json::to_value(record)
            .map_err(|error| ArtifactError::Invalid(error.to_string()))?,
    })
}

/// One compact JSON object per line, in ascending immutable event sequence.
pub fn render_events_jsonl(run_id: RunId, events: &[Event]) -> Result<String, ArtifactError> {
    let events = normalize_events(run_id, events.to_vec())?;
    let mut output = String::new();
    for event in events {
        output.push_str(
            &serde_json::to_string(&ExportEventLine {
                export_schema_version: EXPORT_SCHEMA_VERSION,
                event: &event,
            })
            .map_err(|error| ArtifactError::Invalid(error.to_string()))?,
        );
        output.push('\n');
    }
    Ok(output)
}

/// Human-readable timeline. Payloads remain available in escaped compact JSON, while common
/// entity and Git references are linked to stable local anchors.
pub fn render_timeline_markdown(run_id: RunId, events: &[Event]) -> Result<String, ArtifactError> {
    render_timeline_with_records(run_id, events, &[])
}

fn render_timeline_with_records(
    run_id: RunId,
    events: &[Event],
    records: &[ExportRecord],
) -> Result<String, ArtifactError> {
    let events = normalize_events(run_id, events.to_vec())?;
    let terminal = has_terminal_outcome(&events);
    let mut output = format!(
        "# Run {} timeline\n\nFormat: Tesselate event timeline v{}; {}.\n\n",
        run_id.0,
        EXPORT_SCHEMA_VERSION,
        if terminal {
            "terminal outcome recorded"
        } else {
            "incomplete snapshot; no terminal outcome is recorded"
        }
    );
    if events.is_empty() {
        output.push_str("No run events are recorded yet.\n\n");
    } else {
        output.push_str(
            "| Sequence | Time (UTC) | Event | Actor | Scope | References | Payload |\n|---:|---|---|---|---|---|---|\n",
        );
    }
    for event in events {
        let event_json = serde_json::to_string(&event.payload)
            .map_err(|error| ArtifactError::Invalid(error.to_string()))?;
        let mut anchors = vec![format!("event-{}", event.sequence)];
        anchors.push(format!("actor-{}", event.actor.id.0));
        if let Some(task_id) = event.task_id {
            anchors.push(format!("task-{}", task_id.0));
        }
        let references = collect_references(&event.payload);
        anchors.extend(references.iter().map(|reference| reference.anchor.clone()));
        anchors.sort();
        anchors.dedup();
        let anchor_markup = anchors
            .iter()
            .map(|anchor| format!("<a id=\"{}\"></a>", escape_html_attr(anchor)))
            .collect::<String>();
        let actor = format!(
            "{} ({}; {})",
            escape_table_cell(&event.actor.display_name),
            role_label(event.actor.role),
            entity_link("actor", &event.actor.id.0.to_string())
        );
        let scope = event
            .task_id
            .map(|task| entity_link("task", &task.0.to_string()))
            .unwrap_or_else(|| "—".into());
        let reference_links = references
            .iter()
            .map(|reference| {
                format!(
                    "<a href=\"#{}\">{}:{}</a>",
                    escape_html_attr(&reference.anchor),
                    escape_html(&reference.kind),
                    escape_html(&reference.label)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!(
            "| {}<a href=\"#event-{}\">{}</a> | {} | {} | {} | {} | {} | {} |\n",
            anchor_markup,
            event.sequence,
            event.sequence,
            escape_table_cell(&event.occurred_at.to_rfc3339()),
            escape_table_cell(&event.event_type),
            actor,
            scope,
            if reference_links.is_empty() {
                "—".into()
            } else {
                reference_links
            },
            escape_table_cell(&event_json)
        ));
    }
    if !records.is_empty() {
        output.push_str("\n## Related durable records\n\n");
        for record in records {
            if let Some(anchor) = record_anchor(&record.record_type, &record.record) {
                output.push_str(&format!("<a id=\"{}\"></a>\n", escape_html_attr(&anchor)));
            }
            output.push_str(&format!(
                "- **{}**: `{}`\n",
                escape_table_cell(&record.record_type),
                escape_table_cell(&canonical_value(&record.record))
            ));
        }
    }
    Ok(output)
}

fn render_records_jsonl(records: &[ExportRecord]) -> Result<String, ArtifactError> {
    let mut output = String::new();
    for record in records {
        output.push_str(
            &serde_json::to_string(&ExportRecordLine {
                export_schema_version: EXPORT_SCHEMA_VERSION,
                record_type: &record.record_type,
                record: &record.record,
            })
            .map_err(|error| ArtifactError::Invalid(error.to_string()))?,
        );
        output.push('\n');
    }
    Ok(output)
}

fn canonical_value(value: &Value) -> String {
    serde_json::to_string(value).expect("JSON values are always serializable")
}

fn record_anchor(record_type: &str, record: &Value) -> Option<String> {
    let anchor_kind = match record_type {
        "actor" => "actor",
        "project" => "project",
        "repository" => "repository",
        "run" => "run",
        "task" => "task",
        "assignment" => "assignment",
        "message" => "message",
        "workspace" => "workspace",
        "workspace_commit" => "commit",
        "review_round" => "review-round",
        "review" => "review",
        "review_finding" | "finding_disposition" => "finding",
        "human_decision" => "decision",
        _ => return None,
    };
    find_record_id(record).map(|id| format!("{anchor_kind}-{id}"))
}

fn find_record_id(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(id)) = object.get("id") {
                if Uuid::parse_str(id).is_ok() {
                    return Some(id.clone());
                }
            }
            for (key, kind) in [
                ("task", "uuid"),
                ("finding", "uuid"),
                ("disposition", "uuid"),
                ("commit", "commit"),
                ("finding_id", "uuid"),
                ("commit_id", "commit"),
            ] {
                if let Some(value) = object.get(key) {
                    if let Some(id) = value.as_str().filter(|id| match kind {
                        "uuid" => Uuid::parse_str(id).is_ok(),
                        _ => {
                            id.len() >= 7
                                && id.len() <= 64
                                && id.chars().all(|character| character.is_ascii_hexdigit())
                        }
                    }) {
                        return Some(id.to_owned());
                    }
                    if let Some(id) = find_record_id(value) {
                        return Some(id);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn normalize_events(run_id: RunId, events: Vec<Event>) -> Result<Vec<Event>, ArtifactError> {
    let mut events = events
        .into_iter()
        .filter(|event| !is_credential_event(&event.event_type))
        .collect::<Vec<_>>();
    if events.iter().any(|event| event.run_id != Some(run_id)) {
        return Err(ArtifactError::Invalid(
            "event is not scoped to the requested run".into(),
        ));
    }
    events.sort_by_key(|event| event.sequence);
    let mut seen = HashSet::new();
    if events.iter().any(|event| !seen.insert(event.sequence)) {
        return Err(ArtifactError::Invalid(
            "event sequence numbers must be unique".into(),
        ));
    }
    for event in &mut events {
        redact_sensitive_fields(&mut event.payload);
    }
    Ok(events)
}

fn is_credential_event(event_type: &str) -> bool {
    event_type.starts_with("credential.") || event_type.starts_with("credentials.")
}

fn role_label(role: crate::core::Role) -> &'static str {
    match role {
        crate::core::Role::Supervisor => "supervisor",
        crate::core::Role::Coder => "coder",
        crate::core::Role::Reviewer => "reviewer",
        crate::core::Role::Cli => "cli",
    }
}

fn redact_sensitive_fields(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut sanitized = Map::new();
            for (key, mut value) in std::mem::take(object) {
                if is_sensitive_key(&key) {
                    continue;
                }
                redact_sensitive_fields(&mut value);
                sanitized.insert(key, value);
            }
            *object = sanitized;
        }
        Value::Array(items) => {
            for item in items {
                redact_sensitive_fields(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized == "path"
        || normalized == "canonicalpath"
        || normalized == "worktreepath"
        || [
            "secret",
            "credential",
            "password",
            "token",
            "verifier",
            "authorization",
            "apikey",
            "privatekey",
        ]
        .iter()
        .any(|needle| normalized.contains(needle))
}

fn has_terminal_outcome(events: &[Event]) -> bool {
    events.iter().any(|event| {
        matches!(
            event.event_type.as_str(),
            "run.completed" | "run.limit_reached" | "run.stopped" | "run.failed" | "run.cancelled"
        )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventReference {
    kind: String,
    label: String,
    anchor: String,
}

fn collect_references(payload: &Value) -> Vec<EventReference> {
    fn visit(value: &Value, key: Option<&str>, refs: &mut Vec<EventReference>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    visit(value, Some(key), refs);
                }
            }
            Value::Array(items) => {
                for item in items {
                    visit(item, key, refs);
                }
            }
            Value::String(value) => {
                let Some(key) = key else { return };
                let kind = match key {
                    "task_id" | "task_ids" | "remaining_task_ids" => "task",
                    "assignment_id" => "assignment",
                    "message_id" => "message",
                    "run_id" => "run",
                    "project_id" => "project",
                    "actor_id" => "actor",
                    "round_id" | "review_round_id" => "review-round",
                    "review_id" => "review",
                    "finding_id" | "previous_finding_ids" => "finding",
                    "human_request_id" | "decision_id" | "request_id" => "decision",
                    "workspace_id" => "workspace",
                    "repository_id" | "repository_ids" => "repository",
                    "commit_id" | "commit_ids" | "base_commit" | "head" => "commit",
                    _ => return,
                };
                let safe = if kind == "commit" {
                    value.len() >= 7
                        && value.len() <= 64
                        && value.chars().all(|c| c.is_ascii_hexdigit())
                } else {
                    Uuid::parse_str(value).is_ok()
                };
                if safe {
                    let label = if kind == "commit" && value.len() > 12 {
                        value[..12].to_owned()
                    } else {
                        value.clone()
                    };
                    refs.push(EventReference {
                        kind: kind.into(),
                        label,
                        anchor: format!("{kind}-{value}"),
                    });
                }
            }
            _ => {}
        }
    }
    let mut references = Vec::new();
    visit(payload, None, &mut references);
    references.sort_by(|left, right| (&left.kind, &left.anchor).cmp(&(&right.kind, &right.anchor)));
    references.dedup();
    references
}

fn entity_link(kind: &str, id: &str) -> String {
    let anchor = format!("{kind}-{id}");
    format!(
        "<a href=\"#{}\">{}:{}</a>",
        escape_html_attr(&anchor),
        escape_html(kind),
        escape_html(id)
    )
}

fn escape_table_cell(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('|', "&#124;")
        .replace('`', "&#96;")
        .replace('\\', "&#92;")
        .replace('\r', "")
        .replace('\n', "<br>")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn escape_html_attr(value: &str) -> String {
    escape_html(value)
}

fn artifact_ref(run_id: RunId, kind: &str, path: &Path) -> ArtifactRef {
    ArtifactRef {
        run_id,
        kind: kind.into(),
        path: path.to_string_lossy().into_owned(),
    }
}

fn ensure_plain_directory(path: &Path) -> Result<(), ArtifactError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(ArtifactError::Invalid(format!(
            "artifact directory {} is not a plain directory",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn is_plain_directory(path: &Path) -> Result<bool, ArtifactError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(ArtifactError::Invalid(format!(
            "artifact directory {} is not a plain directory",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn write_atomic(destination: &Path, contents: &[u8]) -> Result<(), io::Error> {
    let parent = destination.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "artifact path has no parent")
    })?;
    let temporary = parent.join(format!(".artifact-{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        drop(file);
        match fs::rename(&temporary, destination) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && destination.exists() => {
                fs::remove_file(destination)?;
                fs::rename(&temporary, destination).map_err(|_| error)
            }
            Err(error) => Err(error),
        }
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    if result.is_ok() {
        File::open(parent)?.sync_all()?;
    }
    result
}

#[cfg(test)]
mod tests;
