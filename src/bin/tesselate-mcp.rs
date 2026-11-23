use std::{env, path::PathBuf};
use tesselate::mcp::serve_stdio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let socket = env::var_os("TESSELATE_SOCKET")
        .map(PathBuf::from)
        .ok_or("TESSELATE_SOCKET must name the coordinator Unix socket")?;
    let credential = env::var("TESSELATE_CREDENTIAL")
        .map_err(|_| "TESSELATE_CREDENTIAL must contain a coordinator-issued credential")?;
    serve_stdio(socket, credential).await
}
