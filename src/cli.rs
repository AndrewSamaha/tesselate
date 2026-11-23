//! Scriptable command-line adapter. All workflow decisions are delegated to `Coordination`.

use crate::{
    config::load_project_config,
    coordination::{application::Application, Coordination},
    core::*,
    mcp::TOOLS,
};
use clap::{Parser, Subcommand, ValueEnum};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_PAGE_SIZE: u32 = 100;

fn parse_uuid(field: &str, value: &str) -> Result<Uuid, CliError> {
    Uuid::parse_str(value).map_err(|_| CoreError::Invalid(format!("{field} must be a UUID")).into())
}

#[derive(Debug, Parser)]
#[command(
    name = "tesselate",
    version,
    about = "Local multi-agent workflow coordinator"
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        env = "TESSELATE_STATE_DIR",
        default_value = ".tesselate"
    )]
    pub state_dir: PathBuf,
    #[arg(long, global = true, env = "TESSELATE_CREDENTIAL_FILE")]
    pub credential_file: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Credential {
        #[command(subcommand)]
        action: CredentialAction,
    },
    Project {
        #[command(subcommand)]
        action: ProjectAction,
    },
    Inspect {
        #[command(subcommand)]
        query: InspectQuery,
    },
    Human {
        #[command(subcommand)]
        action: HumanAction,
    },
    Workspace {
        #[command(subcommand)]
        action: WorkspaceAction,
    },
    Artifact {
        #[command(subcommand)]
        action: ArtifactAction,
    },
    /// Start the one shared coordinator for MCP harness sessions.
    Serve,
    /// Invoke any versioned coordination operation using its JSON params contract.
    Call {
        tool: String,
        #[arg(long, default_value = "{}")]
        params: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum CredentialAction {
    /// Create the initial supervisor credential and store it privately.
    Bootstrap {
        #[arg(long, default_value = "local-supervisor")]
        actor_name: String,
    },
    /// Issue a coder, reviewer, or CLI credential from a supervisor credential.
    Issue {
        #[arg(long)]
        actor_name: String,
        #[arg(long, value_enum)]
        role: CredentialRole,
        #[arg(long)]
        idempotency_key: String,
    },
    Revoke {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CredentialRole {
    Coder,
    Reviewer,
    Cli,
}

#[derive(Debug, Subcommand)]
pub enum ProjectAction {
    Register {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum InspectQuery {
    Projects {
        #[command(flatten)]
        page: PageArgs,
    },
    Repositories {
        #[arg(long)]
        project_id: String,
        #[command(flatten)]
        page: PageArgs,
    },
    Agents {
        #[arg(long)]
        project_id: String,
        #[command(flatten)]
        page: PageArgs,
    },
    Runs {
        #[arg(long)]
        project_id: String,
        #[command(flatten)]
        page: PageArgs,
    },
    Tasks {
        #[arg(long)]
        run_id: String,
        #[command(flatten)]
        page: PageArgs,
    },
    Human {
        #[arg(long)]
        project_id: String,
        #[command(flatten)]
        page: PageArgs,
    },
    Workspace {
        #[arg(long)]
        workspace_id: String,
    },
}

#[derive(Debug, clap::Args)]
pub struct PageArgs {
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    pub limit: u32,
    #[arg(long)]
    pub after: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum HumanAction {
    Resolve {
        #[arg(long)]
        request_id: String,
        #[arg(long)]
        choice: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkspaceAction {
    Request {
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        repository_id: String,
        #[arg(long)]
        base_commit: String,
        #[arg(long)]
        idempotency_key: String,
    },
    Inspect {
        #[arg(long)]
        workspace_id: String,
    },
    Clean {
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        idempotency_key: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ArtifactAction {
    Export {
        #[arg(long)]
        run_id: String,
    },
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub fn prepare_state_dir(path: &Path) -> Result<PathBuf, CliError> {
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "state directory must not be a symlink or non-directory",
        )
        .into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(fs::canonicalize(path)?)
}

pub fn execute(
    app: &Application,
    state_dir: &Path,
    credential_file: Option<&Path>,
    command: Command,
) -> Result<Value, CliError> {
    match command {
        Command::Credential { action } => match action {
            CredentialAction::Bootstrap { actor_name } => {
                let bootstrap_secret = app.prepare_bootstrap()?;
                let issued = app.bootstrap_supervisor(
                    &bootstrap_secret,
                    &actor_name,
                    IdempotencyKey(format!("bootstrap:{}", Uuid::new_v4())),
                )?;
                credential_response(state_dir, issued)
            }
            CredentialAction::Issue {
                actor_name,
                role,
                idempotency_key,
            } => {
                let ctx = supervisor_context(app, credential_file)?;
                let key = IdempotencyKey(idempotency_key);
                let issued = match role {
                    CredentialRole::Coder => {
                        app.issue_agent(&ctx, &actor_name, AgentRole::Coder, key)?
                    }
                    CredentialRole::Reviewer => {
                        app.issue_agent(&ctx, &actor_name, AgentRole::Reviewer, key)?
                    }
                    CredentialRole::Cli => app.issue_cli(&ctx, &actor_name, key)?,
                };
                credential_response(state_dir, issued)
            }
            CredentialAction::Revoke {
                session_id,
                idempotency_key,
            } => {
                let ctx = supervisor_context(app, credential_file)?;
                let session_id = SessionId(parse_uuid("session_id", &session_id)?);
                app.revoke_session(&ctx, session_id, IdempotencyKey(idempotency_key))?;
                Ok(json!({"session_id":session_id,"revoked":true}))
            }
        },
        Command::Project { action } => match action {
            ProjectAction::Register {
                config,
                idempotency_key,
            } => {
                let ctx = supervisor_context(app, credential_file)?;
                let path = fs::canonicalize(&config)?;
                let validated = load_project_config(&path)?;
                to_value(app.register_project(
                    &ctx,
                    validated,
                    path,
                    IdempotencyKey(idempotency_key),
                )?)
            }
        },
        Command::Inspect { query } => {
            let ctx = authenticated_context(app, credential_file)?;
            match query {
                InspectQuery::Projects { page } => {
                    to_value(app.list_projects(&ctx, page.into_request())?)
                }
                InspectQuery::Repositories { project_id, page } => {
                    to_value(app.list_repositories(
                        &ctx,
                        ProjectId(parse_uuid("project_id", &project_id)?),
                        page.into_request(),
                    )?)
                }
                InspectQuery::Agents { project_id, page } => to_value(app.list_agents(
                    &ctx,
                    ProjectId(parse_uuid("project_id", &project_id)?),
                    page.into_request(),
                )?),
                InspectQuery::Runs { project_id, page } => to_value(app.list_runs(
                    &ctx,
                    ProjectId(parse_uuid("project_id", &project_id)?),
                    page.into_request(),
                )?),
                InspectQuery::Tasks { run_id, page } => to_value(app.list_tasks(
                    &ctx,
                    RunId(parse_uuid("run_id", &run_id)?),
                    page.into_request(),
                )?),
                InspectQuery::Human { project_id, page } => to_value(app.list_human_requests(
                    &ctx,
                    ListHumanRequests {
                        project_id: ProjectId(parse_uuid("project_id", &project_id)?),
                        page: page.into_request(),
                    },
                )?),
                InspectQuery::Workspace { workspace_id } => to_value(app.inspect_workspace(
                    &ctx,
                    InspectWorkspace {
                        workspace_id: WorkspaceId(parse_uuid("workspace_id", &workspace_id)?),
                    },
                )?),
            }
        }
        Command::Human { action } => {
            let ctx = authenticated_context(app, credential_file)?;
            match action {
                HumanAction::Resolve {
                    request_id,
                    choice,
                    reason,
                    idempotency_key,
                } => to_value(app.resolve_human(
                    &ctx,
                    ResolveHumanRequest {
                        request_id: HumanRequestId(parse_uuid("request_id", &request_id)?),
                        choice,
                        reason,
                    },
                    IdempotencyKey(idempotency_key),
                )?),
            }
        }
        Command::Workspace { action } => {
            let ctx = authenticated_context(app, credential_file)?;
            match action {
                WorkspaceAction::Request {
                    task_id,
                    repository_id,
                    base_commit,
                    idempotency_key,
                } => to_value(app.request_workspace(
                    &ctx,
                    WorkspaceRequest {
                        task_id: TaskId(parse_uuid("task_id", &task_id)?),
                        repository_id: RepositoryId(parse_uuid("repository_id", &repository_id)?),
                        base_commit,
                    },
                    IdempotencyKey(idempotency_key),
                )?),
                WorkspaceAction::Inspect { workspace_id } => to_value(app.inspect_workspace(
                    &ctx,
                    InspectWorkspace {
                        workspace_id: WorkspaceId(parse_uuid("workspace_id", &workspace_id)?),
                    },
                )?),
                WorkspaceAction::Clean {
                    workspace_id,
                    idempotency_key,
                } => to_value(app.cleanup_workspace(
                    &ctx,
                    CleanupWorkspace {
                        workspace_id: WorkspaceId(parse_uuid("workspace_id", &workspace_id)?),
                    },
                    IdempotencyKey(idempotency_key),
                )?),
            }
        }
        Command::Artifact { action } => {
            let ctx = authenticated_context(app, credential_file)?;
            match action {
                ArtifactAction::Export { run_id } => {
                    let exported = app.export_run(&ctx, RunId(parse_uuid("run_id", &run_id)?))?;
                    Ok(json!({
                        "run_id":exported.run_id,
                        "schema_version":exported.schema_version,
                        "event_count":exported.event_count,
                        "record_count":exported.record_count,
                        "terminal_outcome_recorded":exported.terminal_outcome_recorded,
                        "references":exported.references
                    }))
                }
            }
        }
        Command::Serve => Err(CoreError::Invalid(
            "serve must be started through the tesselate executable".into(),
        )
        .into()),
        Command::Call {
            tool,
            params,
            idempotency_key,
        } => {
            let ctx = authenticated_context(app, credential_file)?;
            if !TOOLS.contains(&tool.as_str()) {
                return Err(
                    CoreError::Invalid(format!("unknown coordination tool {tool:?}")).into(),
                );
            }
            let params: Value = serde_json::from_str(&params)?;
            dispatch(
                app,
                &ctx,
                &tool,
                params,
                idempotency_key.map(IdempotencyKey),
            )
        }
    }
}

impl PageArgs {
    fn into_request(self) -> PageRequest {
        PageRequest {
            limit: self.limit,
            after: self.after.map(EventCursor),
        }
    }
}

fn authenticated_context(
    app: &Application,
    credential_file: Option<&Path>,
) -> Result<SessionContext, CliError> {
    let path = credential_file.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "this operation requires --credential-file or TESSELATE_CREDENTIAL_FILE",
        )
    })?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "credential file must be a regular file, not a symlink",
        )
        .into());
    }
    let secret = fs::read_to_string(path)?;
    app.authenticate(secret.trim()).map_err(Into::into)
}

fn supervisor_context(
    app: &Application,
    credential_file: Option<&Path>,
) -> Result<SessionContext, CliError> {
    let ctx = authenticated_context(app, credential_file)?;
    if ctx.role() != Role::Supervisor {
        return Err(CoreError::Forbidden.into());
    }
    Ok(ctx)
}

fn credential_response(state_dir: &Path, issued: IssuedCredential) -> Result<Value, CliError> {
    let credential_file = if let Some(secret) = &issued.secret {
        Some(store_credential(state_dir, issued.session_id, secret)?)
    } else {
        None
    };
    Ok(json!({
        "session_id":issued.session_id,
        "actor_id":issued.actor_id,
        "credential_file":credential_file,
        "credential":issued.secret,
        "replayed":issued.replayed
    }))
}

fn store_credential(
    state_dir: &Path,
    session_id: SessionId,
    secret: &str,
) -> Result<PathBuf, CliError> {
    let directory = state_dir.join("credentials");
    fs::create_dir_all(&directory)?;
    let metadata = fs::symlink_metadata(&directory)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "credential directory must not be a symlink or non-directory",
        )
        .into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    }
    let path = directory.join(format!("{}.token", session_id.0));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    file.write_all(secret.as_bytes())?;
    file.sync_all()?;
    Ok(path)
}

fn to_value<T: Serialize>(value: T) -> Result<Value, CliError> {
    Ok(serde_json::to_value(value)?)
}

fn decode<T: DeserializeOwned>(value: Value) -> CoreResult<T> {
    serde_json::from_value(value).map_err(|error| CoreError::Invalid(error.to_string()))
}

fn dispatch(
    app: &Application,
    ctx: &SessionContext,
    tool: &str,
    params: Value,
    key: Option<IdempotencyKey>,
) -> Result<Value, CliError> {
    macro_rules! read {
        ($ty:ty, $call:expr) => {{
            let request = decode::<$ty>(params)?;
            to_value(($call)(app, ctx, request)?)
        }};
    }
    macro_rules! mutation {
        ($ty:ty, $call:expr) => {{
            let request = decode::<$ty>(params)?;
            let key = key
                .clone()
                .ok_or_else(|| CoreError::Invalid("mutations require --idempotency-key".into()))?;
            to_value(($call)(app, ctx, request, key)?)
        }};
    }
    match tool {
        "tesselate.v1.projects.list" => {
            read!(PageRequest, |a: &Application, c, p| a.list_projects(c, p))
        }
        "tesselate.v1.repositories.list" => {
            #[derive(Deserialize)]
            struct Request {
                project_id: ProjectId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.list_repositories(ctx, request.project_id, request.page)?)
        }
        "tesselate.v1.agents.list" => {
            #[derive(Deserialize)]
            struct Request {
                project_id: ProjectId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.list_agents(ctx, request.project_id, request.page)?)
        }
        "tesselate.v1.runs.list" => {
            #[derive(Deserialize)]
            struct Request {
                project_id: ProjectId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.list_runs(ctx, request.project_id, request.page)?)
        }
        "tesselate.v1.runs.create" => {
            mutation!(CreateRun, |a: &Application, c, r, k| a.create_run(c, r, k))
        }
        "tesselate.v1.work.available" => {
            #[derive(Deserialize)]
            struct Request {
                project_id: ProjectId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.available_work(ctx, request.project_id, request.page)?)
        }
        "tesselate.v1.tasks.list" => {
            #[derive(Deserialize)]
            struct Request {
                run_id: RunId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.list_tasks(ctx, request.run_id, request.page)?)
        }
        "tesselate.v1.tasks.create" => mutation!(CreateTask, |a: &Application, c, r, k| a
            .create_task(c, r, k)),
        "tesselate.v1.tasks.create_with_dependencies" => {
            mutation!(CreateTaskWithDependencies, |a: &Application, c, r, k| a
                .create_task_with_dependencies(c, r, k))
        }
        "tesselate.v1.tasks.assign" => mutation!(AssignTask, |a: &Application, c, r, k| a
            .assign_task(c, r, k)),
        "tesselate.v1.tasks.claim" => {
            mutation!(ClaimTask, |a: &Application, c, r, k| a.claim_task(c, r, k))
        }
        "tesselate.v1.tasks.release" => mutation!(ReleaseTask, |a: &Application, c, r, k| a
            .release_task(c, r, k)),
        "tesselate.v1.tasks.progress" => mutation!(UpdateProgress, |a: &Application, c, r, k| a
            .update_progress(c, r, k)),
        "tesselate.v1.tasks.submit" => mutation!(SubmitTask, |a: &Application, c, r, k| a
            .submit_task(c, r, k)),
        "tesselate.v1.tasks.disposition" => {
            mutation!(FinalTaskDisposition, |a: &Application, c, r, k| a
                .record_task_disposition(c, r, k))
        }
        "tesselate.v1.runs.complete" => mutation!(CompleteRun, |a: &Application, c, r, k| a
            .complete_run(c, r, k)),
        "tesselate.v1.messages.send" => mutation!(SendMessage, |a: &Application, c, r, k| a
            .send_message(c, r, k)),
        "tesselate.v1.messages.read" => {
            read!(ReadMessages, |a: &Application, c, r| a.read_messages(c, r))
        }
        "tesselate.v1.workspaces.request" => {
            mutation!(WorkspaceRequest, |a: &Application, c, r, k| a
                .request_workspace(c, r, k))
        }
        "tesselate.v1.reviews.request" => mutation!(ReviewRequest, |a: &Application, c, r, k| a
            .request_review(c, r, k)),
        "tesselate.v1.reviews.submit" => mutation!(SubmitReview, |a: &Application, c, r, k| a
            .submit_review(c, r, k)),
        "tesselate.v1.reviews.list" => {
            read!(ListReviews, |a: &Application, c, r| a.list_reviews(c, r))
        }
        "tesselate.v1.reviews.rounds.list" => read!(ListReviewRounds, |a: &Application, c, r| a
            .list_review_rounds(c, r)),
        "tesselate.v1.reviews.disposition" => {
            mutation!(DispositionFindings, |a: &Application, c, r, k| a
                .disposition_findings(c, r, k))
        }
        "tesselate.v1.human.request" => mutation!(HumanRequest, |a: &Application, c, r, k| a
            .request_human(c, r, k)),
        "tesselate.v1.human.list" => read!(ListHumanRequests, |a: &Application, c, r| a
            .list_human_requests(c, r)),
        "tesselate.v1.human.resolve" => {
            mutation!(ResolveHumanRequest, |a: &Application, c, r, k| a
                .resolve_human(c, r, k))
        }
        "tesselate.v1.workspaces.inspect" => read!(InspectWorkspace, |a: &Application, c, r| a
            .inspect_workspace(c, r)),
        "tesselate.v1.workspaces.commits.capture" => {
            mutation!(CaptureWorkspaceCommits, |a: &Application, c, r, k| a
                .capture_commit_refs(c, r, k))
        }
        "tesselate.v1.workspaces.cleanup" => {
            mutation!(CleanupWorkspace, |a: &Application, c, r, k| a
                .cleanup_workspace(c, r, k))
        }
        "tesselate.v1.timeline.read" => {
            #[derive(Deserialize)]
            struct Request {
                scope: EventScope,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.timeline(ctx, request.scope, request.page)?)
        }
        "tesselate.v1.artifacts.locate" => {
            #[derive(Deserialize)]
            struct Request {
                run_id: RunId,
                page: PageRequest,
            }
            let request = decode::<Request>(params)?;
            to_value(app.locate_artifacts(ctx, request.run_id, request.page)?)
        }
        _ => Err(CoreError::Invalid(format!("unsupported tool {tool:?}")).into()),
    }
}

pub fn print_json(value: &Value) -> io::Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")
}

pub fn is_mutation(tool: &str) -> bool {
    !matches!(
        tool,
        "tesselate.v1.projects.list"
            | "tesselate.v1.repositories.list"
            | "tesselate.v1.agents.list"
            | "tesselate.v1.runs.list"
            | "tesselate.v1.work.available"
            | "tesselate.v1.tasks.list"
            | "tesselate.v1.messages.read"
            | "tesselate.v1.reviews.list"
            | "tesselate.v1.reviews.rounds.list"
            | "tesselate.v1.human.list"
            | "tesselate.v1.workspaces.inspect"
            | "tesselate.v1.timeline.read"
            | "tesselate.v1.artifacts.locate"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{coordination::Coordination, persistence::Database};
    use std::{fs, path::PathBuf, process::Command as ProcessCommand};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("tesselate-f09-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn app(&self) -> Application {
            let state = prepare_state_dir(&self.0.join("state")).unwrap();
            let db = Database::open(state.join("coordinator.sqlite3")).unwrap();
            Application::new(db, state.join("worktrees")).unwrap()
        }

        fn repository(&self) -> (PathBuf, String) {
            let repo = self.0.join("repo");
            fs::create_dir_all(&repo).unwrap();
            for args in [
                vec!["init", "--quiet"],
                vec!["config", "user.email", "test@example.invalid"],
                vec!["config", "user.name", "Test"],
            ] {
                assert!(ProcessCommand::new("git")
                    .args(args)
                    .current_dir(&repo)
                    .status()
                    .unwrap()
                    .success());
            }
            fs::write(repo.join("README.md"), "initial\n").unwrap();
            assert!(ProcessCommand::new("git")
                .args(["add", "README.md"])
                .current_dir(&repo)
                .status()
                .unwrap()
                .success());
            assert!(ProcessCommand::new("git")
                .args(["commit", "--quiet", "-m", "initial"])
                .current_dir(&repo)
                .status()
                .unwrap()
                .success());
            let output = ProcessCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&repo)
                .output()
                .unwrap();
            (
                repo,
                String::from_utf8(output.stdout).unwrap().trim().into(),
            )
        }

        fn head(repo: &Path) -> String {
            let output = ProcessCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap().trim().into()
        }

        fn config(&self, repo: &Path, completion_gate: bool) -> PathBuf {
            let config = self.0.join("project.toml");
            fs::write(
                &config,
                format!(
                    r#"name = "cli-project"
artifact_dir = "artifacts"
[[repositories]]
id = "main"
path = {:?}
[workflow]
max_duration_hours = 24
max_concurrent_assignments = 2
max_task_retries = 2
max_review_rounds = 3
[review]
min_independent_reviewers = 2
[human]
completion_approval = {completion_gate}
"#,
                    repo.to_string_lossy()
                ),
            )
            .unwrap();
            config
        }

        fn supervisor(&self, app: &Application) -> PathBuf {
            let state = self.0.join("state");
            let issued = app
                .bootstrap_supervisor(
                    &app.prepare_bootstrap().unwrap(),
                    "supervisor",
                    IdempotencyKey("bootstrap-test".into()),
                )
                .unwrap();
            store_credential(&state, issued.session_id, issued.secret.as_deref().unwrap()).unwrap()
        }

        fn register(
            &self,
            app: &Application,
            credential: &Path,
            completion_gate: bool,
        ) -> ProjectId {
            let (repo, _) = self.repository();
            let config = self.config(&repo, completion_gate);
            let value = execute(
                app,
                &self.0.join("state"),
                Some(credential),
                Command::Project {
                    action: ProjectAction::Register {
                        config,
                        idempotency_key: "register-project".into(),
                    },
                },
            )
            .unwrap();
            serde_json::from_value(value["value"]["id"].clone()).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn registration_json_reads_and_gated_completion_resume_through_cli() {
        let fixture = Fixture::new();
        let app = fixture.app();
        let credential = fixture.supervisor(&app);
        let project_id = fixture.register(&app, &credential, true);

        let projects = execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            Command::Inspect {
                query: InspectQuery::Projects {
                    page: PageArgs {
                        limit: 10,
                        after: None,
                    },
                },
            },
        )
        .unwrap();
        let json = serde_json::to_vec(&projects).unwrap();
        let decoded: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded["items"][0]["id"], json!(project_id));

        let run = execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            Command::Call {
                tool: "tesselate.v1.runs.create".into(),
                params: json!({"project_id":project_id,"goal":"test gate"}).to_string(),
                idempotency_key: Some("create-run".into()),
            },
        )
        .unwrap();
        let run_id: RunId = serde_json::from_value(run["value"]["id"].clone()).unwrap();
        let completion = || Command::Call {
            tool: "tesselate.v1.runs.complete".into(),
            params: json!({"run_id":run_id,"outstanding_integration_steps":[]}).to_string(),
            idempotency_key: Some("complete-run".into()),
        };
        assert!(matches!(
            execute(
                &app,
                &fixture.0.join("state"),
                Some(&credential),
                completion()
            ),
            Err(CliError::Core(CoreError::Recoverable(_)))
        ));

        let pending = execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            Command::Inspect {
                query: InspectQuery::Human {
                    project_id: project_id.0.to_string(),
                    page: PageArgs {
                        limit: 10,
                        after: None,
                    },
                },
            },
        )
        .unwrap();
        let request_id: HumanRequestId =
            serde_json::from_value(pending["items"][0]["id"].clone()).unwrap();
        execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            Command::Human {
                action: HumanAction::Resolve {
                    request_id: request_id.0.to_string(),
                    choice: "approve".into(),
                    reason: "Reviewed completion criteria".into(),
                    idempotency_key: "approve-completion".into(),
                },
            },
        )
        .unwrap();
        let completed = execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            completion(),
        )
        .unwrap();
        assert_eq!(completed["value"]["status"], "completed");

        let exported = execute(
            &app,
            &fixture.0.join("state"),
            Some(&credential),
            Command::Artifact {
                action: ArtifactAction::Export {
                    run_id: run_id.0.to_string(),
                },
            },
        )
        .unwrap();
        assert!(exported["event_count"].as_u64().unwrap() >= 3);
    }

    #[test]
    fn cli_workspace_cleanup_refuses_dirty_worktree() {
        let fixture = Fixture::new();
        let app = fixture.app();
        let supervisor_credential = fixture.supervisor(&app);
        let project = fixture.register(&app, &supervisor_credential, false);
        let supervisor = app
            .authenticate(&fs::read_to_string(&supervisor_credential).unwrap())
            .unwrap();
        let repositories = app
            .list_repositories(
                &supervisor,
                project,
                PageRequest {
                    limit: 10,
                    after: None,
                },
            )
            .unwrap();
        let repository = repositories.items[0].clone();
        let base_commit = Fixture::head(Path::new(&repository.canonical_path));
        let run = app
            .create_run(
                &supervisor,
                CreateRun {
                    project_id: project,
                    goal: "workspace".into(),
                },
                IdempotencyKey("run".into()),
            )
            .unwrap()
            .value;
        let task = app
            .create_task(
                &supervisor,
                CreateTask {
                    run_id: run.id,
                    title: "task".into(),
                    goal: "edit".into(),
                },
                IdempotencyKey("task".into()),
            )
            .unwrap()
            .value;
        let coder = app
            .issue_agent(
                &supervisor,
                "coder",
                AgentRole::Coder,
                IdempotencyKey("coder".into()),
            )
            .unwrap();
        let coder_secret = coder.secret.unwrap();
        let coder_context = app.authenticate(&coder_secret).unwrap();
        app.assign_task(
            &supervisor,
            AssignTask {
                task_id: task.id,
                assignee: coder.actor_id,
            },
            IdempotencyKey("assign".into()),
        )
        .unwrap();
        app.claim_task(
            &coder_context,
            ClaimTask { task_id: task.id },
            IdempotencyKey("claim".into()),
        )
        .unwrap();
        let workspace = app
            .request_workspace(
                &coder_context,
                WorkspaceRequest {
                    task_id: task.id,
                    repository_id: repository.id,
                    base_commit,
                },
                IdempotencyKey("workspace".into()),
            )
            .unwrap()
            .value;
        let inspection = execute(
            &app,
            &fixture.0.join("state"),
            Some(&supervisor_credential),
            Command::Workspace {
                action: WorkspaceAction::Inspect {
                    workspace_id: workspace.id.0.to_string(),
                },
            },
        )
        .unwrap();
        assert_eq!(inspection["workspace"]["id"], json!(workspace.id));
        fs::write(
            Path::new(&workspace.path).join("dirty.txt"),
            "uncommitted\n",
        )
        .unwrap();
        let error = execute(
            &app,
            &fixture.0.join("state"),
            Some(&supervisor_credential),
            Command::Workspace {
                action: WorkspaceAction::Clean {
                    workspace_id: workspace.id.0.to_string(),
                    idempotency_key: "clean-dirty".into(),
                },
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("uncommitted changes"), "{error}");
        fs::remove_file(Path::new(&workspace.path).join("dirty.txt")).unwrap();
        let cleaned = execute(
            &app,
            &fixture.0.join("state"),
            Some(&supervisor_credential),
            Command::Workspace {
                action: WorkspaceAction::Clean {
                    workspace_id: workspace.id.0.to_string(),
                    idempotency_key: "clean-after-dirty".into(),
                },
            },
        )
        .unwrap();
        assert_eq!(cleaned["value"]["id"], json!(workspace.id));
    }

    #[test]
    fn cli_credentials_are_returned_once_and_stored_in_private_files() {
        let fixture = Fixture::new();
        let app = fixture.app();
        let state = fixture.0.join("state");
        let bootstrap = execute(
            &app,
            &state,
            None,
            Command::Credential {
                action: CredentialAction::Bootstrap {
                    actor_name: "supervisor".into(),
                },
            },
        )
        .unwrap();
        let supervisor_path = PathBuf::from(bootstrap["credential_file"].as_str().unwrap());
        assert_eq!(
            fs::read_to_string(&supervisor_path).unwrap(),
            bootstrap["credential"].as_str().unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&supervisor_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let issued = execute(
            &app,
            &state,
            Some(&supervisor_path),
            Command::Credential {
                action: CredentialAction::Issue {
                    actor_name: "coder".into(),
                    role: CredentialRole::Coder,
                    idempotency_key: "issue-coder".into(),
                },
            },
        )
        .unwrap();
        assert!(issued["credential"].as_str().is_some());
        let coder_path = PathBuf::from(issued["credential_file"].as_str().unwrap());
        assert_eq!(
            fs::read_to_string(&coder_path).unwrap(),
            issued["credential"].as_str().unwrap()
        );
        let replay = execute(
            &app,
            &state,
            Some(&supervisor_path),
            Command::Credential {
                action: CredentialAction::Issue {
                    actor_name: "coder".into(),
                    role: CredentialRole::Coder,
                    idempotency_key: "issue-coder".into(),
                },
            },
        )
        .unwrap();
        assert!(replay["credential"].is_null());
        assert!(replay["replayed"].as_bool().unwrap());
    }

    #[test]
    fn clap_surface_parses_machine_read_command_and_mutation_key() {
        let parsed =
            Cli::try_parse_from(["tesselate", "inspect", "projects", "--limit", "5"]).unwrap();
        assert!(matches!(
            parsed.command,
            Command::Inspect {
                query: InspectQuery::Projects { .. }
            }
        ));

        let parsed = Cli::try_parse_from([
            "tesselate",
            "call",
            "tesselate.v1.runs.create",
            "--params",
            "{}",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Call {
                idempotency_key: None,
                ..
            }
        ));
    }
}
