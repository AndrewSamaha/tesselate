use super::{ToolError, ToolOutcome, ToolResponse, PROTOCOL_VERSION, TOOLS};
use crate::{
    coordination::Coordination,
    core::{
        AssignTask, CaptureWorkspaceCommits, ClaimTask, CleanupWorkspace, CompleteRun, CoreError,
        CoreResult, CreateRun, CreateTask, CreateTaskWithDependencies, DispositionFindings,
        EventScope, FinalTaskDisposition, HumanRequest, IdempotencyKey, InspectWorkspace,
        ListHumanRequests, ListReviewRounds, ListReviews, PageRequest, ReadMessages, ReleaseTask,
        ResolveHumanRequest, ReviewRequest, SendMessage, SessionAuthenticator, SessionContext,
        SubmitReview, SubmitTask, UpdateProgress, WorkspaceRequest,
    },
};
use rmcp::{
    handler::server::ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, JsonObject,
        ListToolsResult, ServerCapabilities, ServerConfig, Tool,
    },
    service::{RequestContext, RoleServer, ServiceExt},
    ErrorData as McpError,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Runs the single coordinator socket inside a dedicated, owner-only state directory.
/// F09 supplies the concrete feature composition.
pub async fn serve_coordinator(
    state_dir: impl AsRef<Path>,
    coordination: Arc<dyn Coordination>,
    authenticator: Arc<dyn SessionAuthenticator>,
) -> io::Result<()> {
    let requested_state_dir = state_dir.as_ref();
    fs::create_dir_all(requested_state_dir)?;
    let metadata = fs::symlink_metadata(requested_state_dir)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "coordinator state directory must be a real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(requested_state_dir, fs::Permissions::from_mode(0o700))?;
    }
    let state_dir = fs::canonicalize(requested_state_dir)?;
    let socket_path = state_dir.join("coordinator.sock");
    let _lock = acquire_socket_lock(&socket_path)?;
    remove_stale_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
    }
    let dispatcher = Arc::new(Dispatcher {
        coordination,
        authenticator,
    });
    loop {
        let (stream, _) = listener.accept().await?;
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            let _ = serve_connection(stream, dispatcher).await;
        });
    }
}

fn acquire_socket_lock(socket_path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let lock_path = socket_path.with_file_name(format!(
            "{}.lock",
            socket_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("coordinator")
        ));
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(lock_path)?;
        use std::os::unix::fs::PermissionsExt;
        lock.set_permissions(fs::Permissions::from_mode(0o600))?;
        use std::os::fd::AsRawFd;
        // The OS releases this lock even after an unclean process exit.
        // SAFETY: `lock` owns the valid file descriptor passed to `flock`.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(lock)
    }
    #[cfg(not(unix))]
    {
        let _ = socket_path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the coordinator transport requires Unix domain sockets",
        ))
    }
}

fn remove_stale_socket(socket_path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(socket_path) {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if !metadata.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "coordinator socket path exists and is not a socket",
                    ));
                }
            }
            fs::remove_file(socket_path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[derive(Serialize, Deserialize)]
struct CoordinatorRequest {
    schema_version: u16,
    request_id: String,
    tool: String,
    bearer_credential: String,
    arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolArguments {
    schema_version: u16,
    request_id: String,
    params: Value,
    #[serde(default)]
    idempotency_key: Option<IdempotencyKey>,
}

struct Dispatcher {
    coordination: Arc<dyn Coordination>,
    authenticator: Arc<dyn SessionAuthenticator>,
}

impl Dispatcher {
    fn dispatch(&self, request: CoordinatorRequest) -> Value {
        let arguments: ToolArguments = match serde_json::from_value(request.arguments) {
            Ok(arguments) => arguments,
            Err(error) => {
                return error_response(
                    request.schema_version,
                    request.request_id,
                    CoreError::Invalid(format!("invalid tool arguments: {error}")),
                );
            }
        };
        let request_id = arguments.request_id.clone();
        if request_id.is_empty() || request.request_id != request_id {
            return error_response(
                PROTOCOL_VERSION,
                request.request_id,
                CoreError::Invalid(
                    "request_id must be nonempty and match the adapter envelope".into(),
                ),
            );
        }
        if request.schema_version != PROTOCOL_VERSION
            || arguments.schema_version != PROTOCOL_VERSION
        {
            return error_response(
                PROTOCOL_VERSION,
                request_id,
                CoreError::Invalid("unsupported_version".into()),
            );
        }
        if !TOOLS.contains(&request.tool.as_str()) {
            return error_response(
                PROTOCOL_VERSION,
                request_id,
                CoreError::Invalid("unknown tool".into()),
            );
        }
        if !arguments.params.is_object() {
            return error_response(
                PROTOCOL_VERSION,
                request_id,
                CoreError::Invalid("tool params must be an object".into()),
            );
        }
        let context = match self.authenticator.authenticate(&request.bearer_credential) {
            Ok(context) => context,
            Err(error) => return error_response(PROTOCOL_VERSION, request_id, error),
        };
        if contains_authority_claim(&arguments.params) {
            return error_response(
                PROTOCOL_VERSION,
                request_id,
                CoreError::Invalid("actor and role are bound to the authenticated session".into()),
            );
        }
        let mutation = is_mutation(&request.tool);
        if mutation
            && arguments
                .idempotency_key
                .as_ref()
                .is_none_or(|key| key.0.is_empty())
        {
            return error_response(
                PROTOCOL_VERSION,
                request_id,
                CoreError::Invalid("mutations require an idempotency_key".into()),
            );
        }
        let result = invoke(
            self.coordination.as_ref(),
            &context,
            &request.tool,
            arguments.params,
            arguments.idempotency_key,
        );
        match result {
            Ok(data) => serde_json::to_value(ToolResponse {
                schema_version: PROTOCOL_VERSION,
                request_id,
                outcome: ToolOutcome::Success(data),
            })
            .unwrap_or_else(|_| {
                error_response(
                    PROTOCOL_VERSION,
                    "".into(),
                    CoreError::Internal("response serialization failed".into()),
                )
            }),
            Err(error) => error_response(PROTOCOL_VERSION, request_id, error),
        }
    }
}

fn contains_authority_claim(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            matches!(key.as_str(), "actor_id" | "role" | "session_context")
                || contains_authority_claim(value)
        }),
        Value::Array(values) => values.iter().any(contains_authority_claim),
        _ => false,
    }
}

fn error_response(version: u16, request_id: String, error: CoreError) -> Value {
    let (code, retryable) = match error {
        CoreError::Unauthenticated => ("unauthenticated", false),
        CoreError::Forbidden => ("forbidden", false),
        CoreError::NotFound => ("not_found", false),
        CoreError::Invalid(ref message) if message == "unsupported_version" => {
            ("unsupported_version", false)
        }
        CoreError::Invalid(_) => ("invalid_request", false),
        CoreError::IdempotencyConflict => ("idempotency_conflict", false),
        CoreError::Recoverable(_) => ("recoverable", true),
        CoreError::Internal(_) => ("internal", true),
    };
    let response = ToolResponse::<Value> {
        schema_version: version,
        request_id,
        outcome: ToolOutcome::Error(ToolError {
            code: code.into(),
            message: error.to_string(),
            retryable,
        }),
    };
    serde_json::to_value(response).unwrap_or(Value::Null)
}

fn is_mutation(tool: &str) -> bool {
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

#[derive(Deserialize)]
struct ProjectPage {
    project_id: crate::core::ProjectId,
    page: PageRequest,
}
#[derive(Deserialize)]
struct RunPage {
    run_id: crate::core::RunId,
    page: PageRequest,
}
#[derive(Deserialize)]
struct TimelinePage {
    scope: EventScope,
    page: PageRequest,
}
#[derive(Deserialize)]
struct ArtifactPage {
    run_id: crate::core::RunId,
    page: PageRequest,
}

fn decode<T: DeserializeOwned>(value: Value) -> CoreResult<T> {
    serde_json::from_value(value).map_err(|error| CoreError::Invalid(error.to_string()))
}
fn encode<T: Serialize>(value: T) -> CoreResult<Value> {
    serde_json::to_value(value).map_err(|error| CoreError::Internal(error.to_string()))
}

fn invoke(
    service: &dyn Coordination,
    ctx: &SessionContext,
    tool: &str,
    params: Value,
    key: Option<IdempotencyKey>,
) -> CoreResult<Value> {
    macro_rules! read {
        ($ty:ty, $call:expr) => {{
            let request = decode::<$ty>(params)?;
            encode(($call)(service, ctx, request)?)
        }};
    }
    macro_rules! mutation {
        ($ty:ty, $call:expr) => {{
            let request = decode::<$ty>(params)?;
            encode(($call)(
                service,
                ctx,
                request,
                key.clone().ok_or_else(|| {
                    CoreError::Invalid("mutations require an idempotency_key".into())
                })?,
            )?)
        }};
    }
    match tool {
        "tesselate.v1.projects.list" => read!(PageRequest, |s: &dyn Coordination, c, p| s
            .list_projects(c, p)),
        "tesselate.v1.repositories.list" => {
            let r = decode::<ProjectPage>(params)?;
            encode(service.list_repositories(ctx, r.project_id, r.page)?)
        }
        "tesselate.v1.agents.list" => {
            let r = decode::<ProjectPage>(params)?;
            encode(service.list_agents(ctx, r.project_id, r.page)?)
        }
        "tesselate.v1.runs.list" => {
            let r = decode::<ProjectPage>(params)?;
            encode(service.list_runs(ctx, r.project_id, r.page)?)
        }
        "tesselate.v1.runs.create" => mutation!(CreateRun, |s: &dyn Coordination, c, r, k| s
            .create_run(c, r, k)),
        "tesselate.v1.work.available" => {
            let r = decode::<ProjectPage>(params)?;
            encode(service.available_work(ctx, r.project_id, r.page)?)
        }
        "tesselate.v1.tasks.list" => {
            let r = decode::<RunPage>(params)?;
            encode(service.list_tasks(ctx, r.run_id, r.page)?)
        }
        "tesselate.v1.tasks.create" => mutation!(CreateTask, |s: &dyn Coordination, c, r, k| s
            .create_task(c, r, k)),
        "tesselate.v1.tasks.create_with_dependencies" => mutation!(
            CreateTaskWithDependencies,
            |s: &dyn Coordination, c, r, k| s.create_task_with_dependencies(c, r, k)
        ),
        "tesselate.v1.tasks.assign" => mutation!(AssignTask, |s: &dyn Coordination, c, r, k| s
            .assign_task(c, r, k)),
        "tesselate.v1.tasks.claim" => mutation!(ClaimTask, |s: &dyn Coordination, c, r, k| s
            .claim_task(c, r, k)),
        "tesselate.v1.tasks.release" => mutation!(ReleaseTask, |s: &dyn Coordination, c, r, k| s
            .release_task(c, r, k)),
        "tesselate.v1.tasks.progress" => {
            mutation!(UpdateProgress, |s: &dyn Coordination, c, r, k| s
                .update_progress(c, r, k))
        }
        "tesselate.v1.tasks.submit" => mutation!(SubmitTask, |s: &dyn Coordination, c, r, k| s
            .submit_task(c, r, k)),
        "tesselate.v1.tasks.disposition" => {
            mutation!(FinalTaskDisposition, |s: &dyn Coordination, c, r, k| s
                .record_task_disposition(c, r, k))
        }
        "tesselate.v1.runs.complete" => mutation!(CompleteRun, |s: &dyn Coordination, c, r, k| s
            .complete_run(c, r, k)),
        "tesselate.v1.messages.send" => mutation!(SendMessage, |s: &dyn Coordination, c, r, k| s
            .send_message(c, r, k)),
        "tesselate.v1.messages.read" => read!(ReadMessages, |s: &dyn Coordination, c, r| s
            .read_messages(c, r)),
        "tesselate.v1.workspaces.request" => {
            mutation!(WorkspaceRequest, |s: &dyn Coordination, c, r, k| s
                .request_workspace(c, r, k))
        }
        "tesselate.v1.reviews.request" => {
            mutation!(ReviewRequest, |s: &dyn Coordination, c, r, k| s
                .request_review(c, r, k))
        }
        "tesselate.v1.reviews.submit" => {
            mutation!(SubmitReview, |s: &dyn Coordination, c, r, k| s
                .submit_review(c, r, k))
        }
        "tesselate.v1.reviews.list" => read!(ListReviews, |s: &dyn Coordination, c, r| s
            .list_reviews(c, r)),
        "tesselate.v1.reviews.rounds.list" => {
            read!(ListReviewRounds, |s: &dyn Coordination, c, r| s
                .list_review_rounds(c, r))
        }
        "tesselate.v1.reviews.disposition" => {
            mutation!(DispositionFindings, |s: &dyn Coordination, c, r, k| s
                .disposition_findings(c, r, k))
        }
        "tesselate.v1.human.request" => mutation!(HumanRequest, |s: &dyn Coordination, c, r, k| s
            .request_human(c, r, k)),
        "tesselate.v1.human.list" => read!(ListHumanRequests, |s: &dyn Coordination, c, r| s
            .list_human_requests(c, r)),
        "tesselate.v1.human.resolve" => {
            mutation!(ResolveHumanRequest, |s: &dyn Coordination, c, r, k| s
                .resolve_human(c, r, k))
        }
        "tesselate.v1.workspaces.inspect" => {
            read!(InspectWorkspace, |s: &dyn Coordination, c, r| s
                .inspect_workspace(c, r))
        }
        "tesselate.v1.workspaces.commits.capture" => {
            mutation!(CaptureWorkspaceCommits, |s: &dyn Coordination, c, r, k| s
                .capture_commit_refs(c, r, k))
        }
        "tesselate.v1.workspaces.cleanup" => {
            mutation!(CleanupWorkspace, |s: &dyn Coordination, c, r, k| s
                .cleanup_workspace(c, r, k))
        }
        "tesselate.v1.timeline.read" => {
            let r = decode::<TimelinePage>(params)?;
            encode(service.timeline(ctx, r.scope, r.page)?)
        }
        "tesselate.v1.artifacts.locate" => {
            let r = decode::<ArtifactPage>(params)?;
            encode(service.locate_artifacts(ctx, r.run_id, r.page)?)
        }
        _ => Err(CoreError::Invalid("unknown tool".into())),
    }
}

async fn serve_connection(stream: UnixStream, dispatcher: Arc<Dispatcher>) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = tokio::io::BufReader::new(read);
    loop {
        let Some(frame) = read_frame(&mut read).await? else {
            return Ok(());
        };
        let request: CoordinatorRequest = match serde_json::from_slice(&frame) {
            Ok(request) => request,
            Err(error) => {
                let response = error_response(
                    PROTOCOL_VERSION,
                    String::new(),
                    CoreError::Invalid(format!("invalid coordinator frame: {error}")),
                );
                write_frame(&mut write, &response).await?;
                continue;
            }
        };
        let request_dispatcher = dispatcher.clone();
        let response = tokio::task::spawn_blocking(move || request_dispatcher.dispatch(request))
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;
        write_frame(&mut write, &response).await?;
    }
}

async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let byte = match reader.read_u8().await {
            Ok(byte) => byte,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof && frame.is_empty() => {
                return Ok(None)
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated frame",
                ))
            }
            Err(error) => return Err(error),
        };
        if byte == b'\n' {
            return Ok(Some(frame));
        }
        if frame.len() >= MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "coordinator frame exceeds the size limit",
            ));
        }
        frame.push(byte);
    }
}

async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: &Value,
) -> io::Result<()> {
    let mut encoded = serde_json::to_vec(value).map_err(io::Error::other)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await
}

/// One harness-session adapter. Its bearer is injected locally and never appears in tool params.
pub struct McpGateway {
    socket_path: PathBuf,
    bearer_credential: String,
}

impl McpGateway {
    pub fn new(socket_path: impl AsRef<Path>, bearer_credential: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_owned(),
            bearer_credential: bearer_credential.into(),
        }
    }

    async fn forward(&self, tool: &str, arguments: Value) -> io::Result<Value> {
        let request_id = arguments
            .get("request_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let request = CoordinatorRequest {
            schema_version: PROTOCOL_VERSION,
            request_id,
            tool: tool.into(),
            bearer_credential: self.bearer_credential.clone(),
            arguments,
        };
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        write_frame(
            &mut stream,
            &serde_json::to_value(request).map_err(io::Error::other)?,
        )
        .await?;
        let Some(response) = read_frame(&mut stream).await? else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "coordinator closed connection",
            ));
        };
        serde_json::from_slice(&response).map_err(io::Error::other)
    }
}

/// Serves the versioned Tesselate tools over this process's MCP stdio streams.
pub async fn serve_stdio(
    socket_path: impl AsRef<Path>,
    bearer_credential: impl Into<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = McpGateway::new(socket_path, bearer_credential);
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

impl ServerHandler for McpGateway {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("tesselate", env!("CARGO_PKG_VERSION")))
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(tool_definitions())))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tool_definitions()
            .into_iter()
            .find(|tool| tool.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        if !TOOLS.contains(&request.name.as_ref()) {
            return Ok(CallToolResult::structured_error(json!({
                "schema_version": PROTOCOL_VERSION,
                "request_id": "",
                "outcome": {"status":"error", "data": {"code":"unknown_tool", "message":"unknown Tesselate tool", "retryable":false}}
            })).into());
        }
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        let response = match self.forward(&request.name, arguments).await {
            Ok(response) => response,
            Err(error) => json!({
                "schema_version": PROTOCOL_VERSION,
                "request_id": "",
                "outcome": {"status":"error", "data": {"code":"unavailable", "message":error.to_string(), "retryable":true}}
            }),
        };
        Ok(CallToolResult::structured(response).into())
    }
}

fn tool_definitions() -> Vec<Tool> {
    TOOLS
        .iter()
        .map(|name| {
            let mut schema = JsonObject::new();
            schema.insert("type".into(), json!("object"));
            schema.insert(
                "properties".into(),
                json!({
                    "schema_version":{"type":"integer","const":PROTOCOL_VERSION},
                    "request_id":{"type":"string","minLength":1},
                    "params":{"type":"object"},
                    "idempotency_key":{"type":"string","minLength":1}
                }),
            );
            let mut required = vec!["schema_version", "request_id", "params"];
            if is_mutation(name) {
                required.push("idempotency_key");
            }
            schema.insert("required".into(), json!(required));
            schema.insert("additionalProperties".into(), json!(false));
            Tool::new(*name, "Tesselate version 1 coordination operation", schema)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definitions_match_the_versioned_contract_and_mark_mutations() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), TOOLS.len());
        let mutation = tools
            .iter()
            .find(|tool| tool.name == "tesselate.v1.tasks.create")
            .unwrap();
        assert!(mutation.input_schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("idempotency_key")));
        let read = tools
            .iter()
            .find(|tool| tool.name == "tesselate.v1.tasks.list")
            .unwrap();
        assert!(!read.input_schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("idempotency_key")));
    }

    #[test]
    fn mutation_classification_covers_every_mutating_coordination_tool() {
        assert!(TOOLS.iter().filter(|name| is_mutation(name)).count() > 0);
        assert!(!is_mutation("tesselate.v1.timeline.read"));
        assert!(!is_mutation("tesselate.v1.work.available"));
        assert!(!is_mutation("tesselate.v1.human.list"));
        assert!(is_mutation("tesselate.v1.human.resolve"));
    }
}
