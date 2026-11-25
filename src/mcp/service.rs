use super::{ToolError, ToolOutcome, ToolResponse, PROTOCOL_VERSION, TOOLS};
use crate::{
    coordination::Coordination,
    core::{
        AssignTask, CaptureWorkspaceCommits, ClaimTask, CleanupWorkspace, CompleteRun, CoreError,
        CoreResult, CreateComparisonRun, CreateRun, CreateTask, CreateTaskWithDependencies,
        DispositionFindings, EventScope, FinalTaskDisposition, HumanRequest, IdempotencyKey,
        InspectWorkspace, ListHumanRequests, ListReviewRounds, ListReviews, PageRequest,
        ReadMessages, ReleaseTask, ResolveHumanRequest, ReviewRequest, SelectCandidate,
        SendMessage, SessionAuthenticator, SessionContext, SubmitReview, SubmitTask, TaskId,
        UpdateProgress, WorkspaceRequest,
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
        if let Err(message) = validate_params(&arguments.params, &params_schema(&request.tool)) {
            return error_response(PROTOCOL_VERSION, request_id, CoreError::Invalid(message));
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

#[derive(Deserialize)]
struct SessionRenewal {}
#[derive(Deserialize)]
struct LeaseRenewal {
    task_id: TaskId,
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
        "tesselate.v1.sessions.renew" => {
            mutation!(SessionRenewal, |s: &dyn Coordination, c, _r, k| s
                .renew_session(c, k))
        }
        "tesselate.v1.assignments.lease.renew" => {
            mutation!(LeaseRenewal, |s: &dyn Coordination, c, r: LeaseRenewal, k| s
                .renew_assignment_lease(c, r.task_id, k))
        }
        "tesselate.v1.runs.create" => mutation!(CreateRun, |s: &dyn Coordination, c, r, k| s
            .create_run(c, r, k)),
        "tesselate.v1.runs.create_comparison" => mutation!(
            CreateComparisonRun,
            |s: &dyn Coordination, c, r, k| s.create_comparison_run(c, r, k)
        ),
        "tesselate.v1.runs.select_candidate" => mutation!(
            SelectCandidate,
            |s: &dyn Coordination, c, r, k| s.select_candidate(c, r, k)
        ),
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
                    "params":params_schema(name),
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

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type":"object",
        "properties":properties,
        "required":required,
        "additionalProperties":false
    })
}

fn string_schema() -> Value {
    json!({"type":"string"})
}

fn id_schema() -> Value {
    json!({"type":"string","format":"uuid"})
}

fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}

fn array_schema(items: Value) -> Value {
    json!({"type":"array","items":items})
}

fn page_schema() -> Value {
    object_schema(
        json!({"limit":{"type":"integer","minimum":1},"after":nullable(string_schema())}),
        &["limit"],
    )
}

fn commit_ref_schema() -> Value {
    object_schema(
        json!({"repository_id":id_schema(),"commit_id":string_schema()}),
        &["repository_id", "commit_id"],
    )
}

fn project_page_schema() -> Value {
    object_schema(
        json!({"project_id":id_schema(),"page":page_schema()}),
        &["project_id", "page"],
    )
}

fn run_page_schema() -> Value {
    object_schema(
        json!({"run_id":id_schema(),"page":page_schema()}),
        &["run_id", "page"],
    )
}

fn params_schema(tool: &str) -> Value {
    let task_id = id_schema();
    let project_id = id_schema();
    let workspace_id = id_schema();
    let findings = object_schema(
        json!({
            "id":id_schema(),
            "severity":{"type":"string","enum":["low","medium","high","critical"]},
            "location":string_schema(),
            "evidence":string_schema(),
            "proposed_correction":string_schema()
        }),
        &[
            "id",
            "severity",
            "location",
            "evidence",
            "proposed_correction",
        ],
    );
    match tool {
        "tesselate.v1.projects.list" => page_schema(),
        "tesselate.v1.repositories.list"
        | "tesselate.v1.agents.list"
        | "tesselate.v1.runs.list"
        | "tesselate.v1.work.available" => project_page_schema(),
        "tesselate.v1.sessions.renew" => object_schema(json!({}), &[]),
        "tesselate.v1.assignments.lease.renew" => {
            object_schema(json!({"task_id":task_id}), &["task_id"])
        }
        "tesselate.v1.runs.create" => object_schema(
            json!({"project_id":project_id,"goal":string_schema()}),
            &["project_id", "goal"],
        ),
        "tesselate.v1.runs.create_comparison" => {
            let workflow = object_schema(
                json!({
                    "candidates":{"type":"integer","const":2},
                    "reviewers_per_candidate":{"type":"integer","minimum":2,"maximum":u32::MAX},
                    "comparison_reviewers":{"type":"integer","const":1},
                    "winner_approval":{"type":"string","const":"human"}
                }),
                &[
                    "candidates",
                    "reviewers_per_candidate",
                    "comparison_reviewers",
                    "winner_approval",
                ],
            );
            object_schema(
                json!({"project_id":project_id,"goal":string_schema(),"workflow":workflow}),
                &["project_id", "goal", "workflow"],
            )
        }
        "tesselate.v1.runs.select_candidate" => object_schema(
            json!({"run_id":id_schema(),"task_id":task_id,"commit_id":string_schema(),"reason":string_schema()}),
            &["run_id", "task_id", "commit_id", "reason"],
        ),
        "tesselate.v1.tasks.list" => run_page_schema(),
        "tesselate.v1.tasks.create" => object_schema(
            json!({"run_id":id_schema(),"title":string_schema(),"goal":string_schema()}),
            &["run_id", "title", "goal"],
        ),
        "tesselate.v1.tasks.create_with_dependencies" => object_schema(
            json!({"run_id":id_schema(),"title":string_schema(),"goal":string_schema(),"dependencies":array_schema(task_id)}),
            &["run_id", "title", "goal", "dependencies"],
        ),
        "tesselate.v1.tasks.assign" => object_schema(
            json!({"task_id":task_id,"assignee":id_schema()}),
            &["task_id", "assignee"],
        ),
        "tesselate.v1.tasks.claim" => object_schema(json!({"task_id":task_id}), &["task_id"]),
        "tesselate.v1.tasks.release" => object_schema(
            json!({"task_id":task_id,"reason":string_schema()}),
            &["task_id", "reason"],
        ),
        "tesselate.v1.tasks.progress" => object_schema(
            json!({"task_id":task_id,"progress":string_schema()}),
            &["task_id", "progress"],
        ),
        "tesselate.v1.tasks.submit" => object_schema(
            json!({"task_id":task_id,"commits":array_schema(commit_ref_schema()),"summary":string_schema()}),
            &["task_id", "commits", "summary"],
        ),
        "tesselate.v1.tasks.disposition" => object_schema(
            json!({"task_id":task_id,"outcome":{"type":"string","enum":["completed","blocked","failed","cancelled"]},"reason":string_schema()}),
            &["task_id", "outcome", "reason"],
        ),
        "tesselate.v1.runs.complete" => object_schema(
            json!({"run_id":id_schema(),"outstanding_integration_steps":array_schema(string_schema())}),
            &["run_id", "outstanding_integration_steps"],
        ),
        "tesselate.v1.messages.send" => object_schema(
            json!({"project_id":project_id,"run_id":nullable(id_schema()),"task_id":nullable(task_id),"body":string_schema()}),
            &["project_id", "body"],
        ),
        "tesselate.v1.messages.read" => object_schema(
            json!({"project_id":project_id,"task_id":nullable(task_id),"page":page_schema()}),
            &["project_id", "page"],
        ),
        "tesselate.v1.workspaces.request" => object_schema(
            json!({"task_id":task_id,"repository_id":id_schema(),"base_commit":string_schema()}),
            &["task_id", "repository_id", "base_commit"],
        ),
        "tesselate.v1.reviews.request" => object_schema(
            json!({"task_id":task_id,"commit_refs":array_schema(commit_ref_schema()),"previous_finding_ids":array_schema(id_schema())}),
            &["task_id", "commit_refs", "previous_finding_ids"],
        ),
        "tesselate.v1.reviews.submit" => object_schema(
            json!({"round_id":id_schema(),"passed":{"type":"boolean"},"summary":string_schema(),"findings":array_schema(findings)}),
            &["round_id", "passed", "summary", "findings"],
        ),
        "tesselate.v1.reviews.list" | "tesselate.v1.reviews.rounds.list" => object_schema(
            json!({"task_id":task_id,"page":page_schema()}),
            &["task_id", "page"],
        ),
        "tesselate.v1.reviews.disposition" => object_schema(
            json!({"task_id":task_id,"dispositions":array_schema(object_schema(
                json!({"finding_id":id_schema(),"disposition":{"type":"string","enum":["fixed","accepted","deferred"]},"reason":string_schema()}),
                &["finding_id", "disposition", "reason"],
            ))}),
            &["task_id", "dispositions"],
        ),
        "tesselate.v1.human.request" => object_schema(
            json!({"project_id":project_id,"run_id":nullable(id_schema()),"task_id":nullable(task_id),"question":string_schema(),"context":string_schema(),"choices":array_schema(string_schema()),"expires_at":nullable(json!({"type":"string","format":"date-time"})),"on_expiry":nullable(json!({"type":"string","enum":["expire","escalate"]}))}),
            &["project_id", "question", "context", "choices"],
        ),
        "tesselate.v1.human.list" => object_schema(
            json!({"project_id":project_id,"page":page_schema()}),
            &["project_id", "page"],
        ),
        "tesselate.v1.human.resolve" => object_schema(
            json!({"request_id":id_schema(),"choice":string_schema(),"reason":string_schema()}),
            &["request_id", "choice", "reason"],
        ),
        "tesselate.v1.workspaces.inspect" | "tesselate.v1.workspaces.cleanup" => {
            object_schema(json!({"workspace_id":workspace_id}), &["workspace_id"])
        }
        "tesselate.v1.workspaces.commits.capture" => object_schema(
            json!({"workspace_id":workspace_id,"expected_head":string_schema()}),
            &["workspace_id", "expected_head"],
        ),
        "tesselate.v1.timeline.read" => object_schema(
            json!({"scope":object_schema(
                json!({"project_id":nullable(project_id),"run_id":nullable(id_schema()),"task_id":nullable(task_id)}),
                &[],
            ),"page":page_schema()}),
            &["scope", "page"],
        ),
        "tesselate.v1.artifacts.locate" => run_page_schema(),
        _ => unreachable!("every public MCP tool must define its params schema"),
    }
}

fn validate_params(params: &Value, schema: &Value) -> Result<(), String> {
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
        if params.is_null() {
            return if any_of.iter().any(|branch| branch["type"] == "null") {
                Ok(())
            } else {
                Err("params has an invalid null value".into())
            };
        }
        if let Some(branch) = any_of.iter().find(|branch| branch["type"] != "null") {
            return validate_params(params, branch);
        }
    }
    if let Some(object) = params.as_object() {
        let properties = schema
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| "params has an invalid object shape".to_owned())?;
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(format!("missing required params field `{name}`"));
                }
            }
        }
        if let Some(name) = object.keys().find(|name| !properties.contains_key(*name)) {
            return Err(format!("unknown params field `{name}`"));
        }
        for (name, value) in object {
            validate_params(value, &properties[name])?;
        }
    } else if let (Some(values), Some(items)) = (params.as_array(), schema.get("items")) {
        for value in values {
            validate_params(value, items)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definitions_match_the_versioned_contract_and_mark_mutations() {
        let tools = tool_definitions();
        assert_eq!(tools.len(), TOOLS.len());
        for tool in &tools {
            let params = &tool.input_schema["properties"]["params"];
            assert_eq!(params["type"], "object", "{} params schema", tool.name);
            assert_eq!(params["additionalProperties"], false, "{}", tool.name);
            assert!(params["properties"].is_object(), "{}", tool.name);
            if tool.name != "tesselate.v1.sessions.renew" {
                assert!(
                    !params["required"].as_array().unwrap().is_empty(),
                    "{}",
                    tool.name
                );
            }
        }
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
        assert_eq!(
            mutation.input_schema["properties"]["params"]["required"],
            json!(["run_id", "title", "goal"])
        );
        assert_eq!(
            read.input_schema["properties"]["params"]["properties"]["page"]["properties"]["after"]
                ["anyOf"][1]["type"],
            "null"
        );
    }

    #[test]
    fn strict_params_reject_unknown_and_missing_fields_at_each_object_level() {
        let task_create = params_schema("tesselate.v1.tasks.create");
        let valid_task = json!({
            "run_id":crate::core::RunId::new(),
            "title":"title",
            "goal":"goal"
        });
        validate_params(&valid_task, &task_create).unwrap();
        let decoded: CreateTask = decode(valid_task.clone()).unwrap();
        assert_eq!(decoded.title, "title");
        let mut extra_mutation_field = valid_task.clone();
        extra_mutation_field["unexpected"] = json!(true);
        assert!(validate_params(&extra_mutation_field, &task_create)
            .unwrap_err()
            .contains("unexpected"));
        let missing_mutation_field = json!({"run_id":crate::core::RunId::new(),"title":"title"});
        assert!(validate_params(&missing_mutation_field, &task_create)
            .unwrap_err()
            .contains("goal"));

        let read_schema = params_schema("tesselate.v1.runs.list");
        let mut extra_nested_field = json!({
            "project_id":crate::core::ProjectId::new(),
            "page":{"limit":20,"after":null,"unexpected":false}
        });
        assert!(validate_params(&extra_nested_field, &read_schema)
            .unwrap_err()
            .contains("unexpected"));
        extra_nested_field["page"] = json!({"limit":20});
        assert!(validate_params(&extra_nested_field, &read_schema).is_ok());
        extra_nested_field["page"] = json!({});
        assert!(validate_params(&extra_nested_field, &read_schema)
            .unwrap_err()
            .contains("limit"));
    }

    #[test]
    fn optional_params_are_omittable_nullable_and_match_serde_decoding() {
        let page = json!({"limit":20});
        validate_params(&page, &page_schema()).unwrap();
        assert_eq!(decode::<PageRequest>(page).unwrap().after, None);

        let project_id = crate::core::ProjectId::new();
        let send = json!({"project_id":project_id,"body":"hello","run_id":null});
        validate_params(&send, &params_schema("tesselate.v1.messages.send")).unwrap();
        let decoded: SendMessage = decode(send).unwrap();
        assert_eq!(decoded.run_id, None);
        assert_eq!(decoded.task_id, None);

        let read = json!({"project_id":project_id,"page":{"limit":10}});
        validate_params(&read, &params_schema("tesselate.v1.messages.read")).unwrap();
        let decoded: ReadMessages = decode(read).unwrap();
        assert_eq!(decoded.task_id, None);
        assert_eq!(decoded.page.after, None);

        let request = json!({"project_id":project_id,"question":"approve?","context":"run","choices":["yes","no"]});
        validate_params(&request, &params_schema("tesselate.v1.human.request")).unwrap();
        let decoded: HumanRequest = decode(request).unwrap();
        assert_eq!(decoded.run_id, None);
        assert_eq!(decoded.task_id, None);
        assert_eq!(decoded.expires_at, None);
        assert_eq!(decoded.on_expiry, None);

        #[derive(Deserialize)]
        struct TimelineRequest {
            scope: EventScope,
            page: PageRequest,
        }
        let timeline = json!({"scope":{},"page":{"limit":10}});
        validate_params(&timeline, &params_schema("tesselate.v1.timeline.read")).unwrap();
        let decoded: TimelineRequest = decode(timeline).unwrap();
        assert_eq!(decoded.scope.project_id, None);
        assert_eq!(decoded.scope.run_id, None);
        assert_eq!(decoded.scope.task_id, None);
        assert_eq!(decoded.page.after, None);

        assert!(validate_params(
            &json!({"project_id":project_id}),
            &params_schema("tesselate.v1.messages.send")
        )
        .unwrap_err()
        .contains("body"));
        assert!(validate_params(&json!({}), &page_schema())
            .unwrap_err()
            .contains("limit"));
    }

    #[test]
    fn mutation_classification_covers_every_mutating_coordination_tool() {
        assert!(TOOLS.iter().filter(|name| is_mutation(name)).count() > 0);
        assert!(!is_mutation("tesselate.v1.timeline.read"));
        assert!(!is_mutation("tesselate.v1.work.available"));
        assert!(!is_mutation("tesselate.v1.human.list"));
        assert!(is_mutation("tesselate.v1.human.resolve"));
    }

    #[test]
    fn renewal_tools_require_keys_and_admit_only_bearer_bound_params() {
        for name in [
            "tesselate.v1.sessions.renew",
            "tesselate.v1.assignments.lease.renew",
        ] {
            assert!(TOOLS.contains(&name));
            assert!(is_mutation(name));
            let tool = tool_definitions()
                .into_iter()
                .find(|tool| tool.name == name)
                .unwrap();
            assert!(tool.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("idempotency_key")));
        }
        let session_schema = params_schema("tesselate.v1.sessions.renew");
        validate_params(&json!({}), &session_schema).unwrap();
        assert!(validate_params(&json!({"secret":"injected"}), &session_schema).is_err());
        let lease_schema = params_schema("tesselate.v1.assignments.lease.renew");
        let task_id = TaskId::new();
        validate_params(&json!({"task_id":task_id}), &lease_schema).unwrap();
        assert!(validate_params(&json!({}), &lease_schema).is_err());
        assert!(validate_params(
            &json!({"task_id":task_id,"actor_id":crate::core::ActorId::new()}),
            &lease_schema
        )
        .is_err());
        assert!(!TOOLS.contains(&"tesselate.v1.sessions.supervisor.recover_local"));
    }
}
