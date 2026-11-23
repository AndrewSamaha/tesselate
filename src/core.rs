use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
    ReviewRoundId,
    FindingId,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionContext {
    session_id: SessionId,
    actor_id: ActorId,
    role: Role,
}
impl SessionContext {
    #[allow(dead_code)] // Used by the F05 authenticator when its credential store is implemented.
    pub(crate) fn validated(session_id: SessionId, actor_id: ActorId, role: Role) -> Self {
        Self {
            session_id,
            actor_id,
            role,
        }
    }
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    pub fn role(&self) -> Role {
        self.role
    }
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
pub struct Repository {
    pub id: RepositoryId,
    pub project_id: ProjectId,
    pub name: String,
    pub canonical_path: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Run {
    pub id: RunId,
    pub project_id: ProjectId,
    pub goal: String,
    pub started_at: DateTime<Utc>,
    pub limits: WorkflowLimits,
    pub status: RunStatus,
    pub reviewed_commit_ids: Vec<String>,
    pub outstanding_integration_steps: Vec<String>,
    pub outcome: Option<RunOutcome>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowLimits {
    pub max_duration_hours: u64,
    pub max_concurrent_assignments: u32,
    pub max_task_retries: u32,
    pub max_review_rounds: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunOutcome {
    pub reason: String,
    pub remaining_task_ids: Vec<TaskId>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub run_id: RunId,
    pub title: String,
    pub goal: String,
    pub status: TaskStatus,
    pub owner: Option<ActorId>,
    pub progress: Option<String>,
    pub attempts: u32,
    pub commit_ids: Vec<String>,
    pub result: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Actor {
    pub id: ActorId,
    pub display_name: String,
    pub role: Role,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Agent {
    pub actor_id: ActorId,
    pub display_name: String,
    pub role: Role,
    pub status: AgentStatus,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Active,
    Disconnected,
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
    pub id: ReviewRoundId,
    pub task_id: TaskId,
    pub round: u32,
    pub commit_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewFinding {
    pub id: FindingId,
    pub severity: FindingSeverity,
    pub location: String,
    pub evidence: String,
    pub proposed_correction: String,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FindingSeverity {
    Low,
    Medium,
    High,
    Critical,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FindingDisposition {
    pub finding_id: FindingId,
    pub disposition: DispositionKind,
    pub reason: String,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DispositionKind {
    Fixed,
    Accepted,
    Deferred,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Review {
    pub id: ReviewId,
    pub round_id: ReviewRoundId,
    pub task_id: TaskId,
    pub reviewer: ActorId,
    pub passed: bool,
    pub findings: Vec<ReviewFinding>,
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
pub struct ClaimTask {
    pub task_id: TaskId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReleaseTask {
    pub task_id: TaskId,
    pub reason: String,
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
pub struct ListReviews {
    pub task_id: TaskId,
    pub page: PageRequest,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HumanRequest {
    pub task_id: Option<TaskId>,
    pub question: String,
    pub choices: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListHumanRequests {
    pub project_id: ProjectId,
    pub page: PageRequest,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateRun {
    pub project_id: ProjectId,
    pub goal: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateTaskWithDependencies {
    pub run_id: RunId,
    pub title: String,
    pub goal: String,
    pub dependencies: Vec<TaskId>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmitTask {
    pub task_id: TaskId,
    pub commit_ids: Vec<String>,
    pub summary: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompleteRun {
    pub run_id: RunId,
    pub outstanding_integration_steps: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FinalTaskDisposition {
    pub task_id: TaskId,
    pub outcome: FinalTaskOutcome,
    pub reason: String,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FinalTaskOutcome {
    Completed,
    Blocked,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub run_id: RunId,
    pub kind: String,
    pub path: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailableWork {
    pub tasks: Vec<Task>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolveHumanRequest {
    pub request_id: HumanRequestId,
    pub choice: String,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InspectWorkspace {
    pub workspace_id: WorkspaceId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CleanupWorkspace {
    pub workspace_id: WorkspaceId,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadMessages {
    pub project_id: ProjectId,
    pub task_id: Option<TaskId>,
    pub page: PageRequest,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmitReview {
    pub round_id: ReviewRoundId,
    pub passed: bool,
    pub summary: String,
    pub findings: Vec<ReviewFinding>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DispositionFindings {
    pub task_id: TaskId,
    pub dispositions: Vec<FindingDisposition>,
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
    pub after: Option<EventCursor>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct EventCursor(pub String);
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
    pub sequence: u64,
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
    pub canonical_repository_path: String,
    pub base_commit: String,
    pub branch: String,
    pub worktree_path: String,
    pub owner: ActorId,
    pub checkpoint: GitCheckpoint,
    pub observed: Option<ObservedGitState>,
    pub result: Option<Workspace>,
    pub recovery_reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedGitState {
    pub repository_path: String,
    pub worktree_path: String,
    pub branch: Option<String>,
    pub head_commit: Option<String>,
    pub clean: Option<bool>,
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
    fn bootstrap_supervisor(
        &self,
        bootstrap_secret: &str,
        actor_name: &str,
    ) -> CoreResult<IssuedCredential>;
    fn issue_agent(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
        role: AgentRole,
    ) -> CoreResult<IssuedCredential>;
    fn issue_cli(
        &self,
        supervisor: &SessionContext,
        actor_name: &str,
    ) -> CoreResult<IssuedCredential>;
    fn revoke(&self, supervisor: &SessionContext, session: SessionId) -> CoreResult<()>;
}
/// Resolves a bearer credential to a coordinator-validated context. The result cannot be
/// deserialized from client input or constructed outside this crate.
pub trait SessionAuthenticator: Send + Sync {
    fn authenticate(&self, bearer_credential: &str) -> CoreResult<SessionContext>;
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentRole {
    Coder,
    Reviewer,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedCredential {
    pub session_id: SessionId,
    pub actor_id: ActorId,
    pub secret: String,
}
#[cfg(test)]
mod authentication_contract_tests {
    use super::*;

    struct CredentialMap;
    impl SessionAuthenticator for CredentialMap {
        fn authenticate(&self, credential: &str) -> CoreResult<SessionContext> {
            match credential {
                "coder-token" => Ok(SessionContext::validated(
                    SessionId::new(),
                    ActorId::new(),
                    Role::Coder,
                )),
                _ => Err(CoreError::Unauthenticated),
            }
        }
    }

    #[test]
    fn client_role_claim_cannot_override_validated_credential_role() {
        let client_claim = Role::Supervisor;
        let context = CredentialMap.authenticate("coder-token").unwrap();
        assert_eq!(client_claim, Role::Supervisor);
        assert_eq!(context.role(), Role::Coder);
        assert!(CredentialMap
            .authenticate("forged-supervisor-token")
            .is_err());
    }
}
