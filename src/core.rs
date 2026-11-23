use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;
use uuid::Uuid;

macro_rules! id_type { ($($name:ident),+ $(,)?) => {$ (#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)] #[serde(transparent)] pub struct $name(pub Uuid); impl $name { pub fn new() -> Self { Self(Uuid::new_v4()) } } impl Default for $name { fn default() -> Self { Self::new() } })+}; }
id_type!(
    ProjectId,
    RepositoryId,
    RunId,
    TaskId,
    ActorId,
    SessionId,
    EventId,
    MessageId,
    ReviewId,
    HumanRequestId,
    WorkspaceId,
    GitOperationId
);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(pub String);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Supervisor,
    Coder,
    Reviewer,
    Cli,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionContext {
    pub session_id: SessionId,
    pub actor_id: ActorId,
    pub role: Role,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Ready,
    Assigned,
    Coding,
    Submitted,
    InReview,
    RevisionRequired,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Active,
    Completed,
    Stopped,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanStatus {
    Pending,
    Resolved,
    Expired,
    Escalated,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("unauthenticated session")]
    Unauthenticated,
    #[error("role is not authorized for this operation")]
    Forbidden,
    #[error("resource not found")]
    NotFound,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("idempotency key reused with a different request")]
    IdempotencyConflict,
    #[error("operation is in a recoverable state: {0}")]
    Recoverable(String),
    #[error("internal error: {0}")]
    Internal(String),
}
pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Run {
    pub id: RunId,
    pub project_id: ProjectId,
    pub status: RunStatus,
    pub reviewed_commit_ids: Vec<String>,
    pub outstanding_integration_steps: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub run_id: RunId,
    pub title: String,
    pub status: TaskStatus,
    pub owner: Option<ActorId>,
    pub progress: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Actor {
    pub id: ActorId,
    pub display_name: String,
    pub role: Role,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub id: MessageId,
    pub project_id: ProjectId,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub sender: ActorId,
    pub body: String,
    pub event_id: EventId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub task_id: TaskId,
    pub repository_id: RepositoryId,
    pub base_commit: String,
    pub branch: String,
    pub path: String,
    pub owner: ActorId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewRound {
    pub id: ReviewId,
    pub task_id: TaskId,
    pub round: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Review {
    pub id: ReviewId,
    pub task_id: TaskId,
    pub reviewer: ActorId,
    pub passed: bool,
    pub event_id: EventId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanDecision {
    pub id: HumanRequestId,
    pub task_id: Option<TaskId>,
    pub status: HumanStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateTask {
    pub run_id: RunId,
    pub title: String,
    pub goal: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssignTask {
    pub task_id: TaskId,
    pub assignee: ActorId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpdateProgress {
    pub task_id: TaskId,
    pub progress: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SendMessage {
    pub project_id: ProjectId,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub body: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceRequest {
    pub task_id: TaskId,
    pub repository_id: RepositoryId,
    pub base_commit: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewRequest {
    pub task_id: TaskId,
    pub commit_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmitReview {
    pub task_id: TaskId,
    pub passed: bool,
    pub summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanRequest {
    pub task_id: Option<TaskId>,
    pub question: String,
    pub choices: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Mutation<T> {
    pub value: T,
    pub event_id: EventId,
    pub replayed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageRequest {
    pub limit: u32,
    pub after: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventScope {
    pub project_id: Option<ProjectId>,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub schema_version: u16,
    pub event_id: EventId,
    pub occurred_at: DateTime<Utc>,
    pub actor: Actor,
    pub project_id: Option<ProjectId>,
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GitOperationIntent {
    pub id: GitOperationId,
    pub assignment_key: String,
    pub repository_id: RepositoryId,
    pub base_commit: String,
    pub branch: String,
    pub worktree_path: String,
    pub owner: ActorId,
    pub phase: GitCheckpoint,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GitCheckpoint {
    Prepared,
    MutationStarted,
    GitApplied,
    Recorded,
    Conflict,
}

pub trait CredentialIssuer: Send + Sync {
    fn bootstrap(
        &self,
        bootstrap_secret: &str,
        actor_name: &str,
        role: Role,
    ) -> CoreResult<IssuedCredential>;
    fn revoke(&self, ctx: &SessionContext, session: SessionId) -> CoreResult<()>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCredential {
    pub session_id: SessionId,
    pub actor_id: ActorId,
    pub secret: String,
}
pub trait EventStore: Send + Sync {
    fn append_mutation<T: Serialize + Debug>(
        &self,
        ctx: &SessionContext,
        key: &IdempotencyKey,
        request: &T,
    ) -> CoreResult<Mutation<serde_json::Value>>;
}
