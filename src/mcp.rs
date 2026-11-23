//! Versioned MCP adapter contract.

use crate::core::{ActorId, IdempotencyKey, Role, SessionId};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
pub const TOOL_NAMESPACE: &str = "tesselate.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<JsonValue>),
    Object(serde_json::Map<String, serde_json::Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolRequest<T> {
    pub schema_version: u16,
    pub request_id: String,
    pub idempotency_key: Option<IdempotencyKey>,
    pub params: T,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResponse<T> {
    pub schema_version: u16,
    pub request_id: String,
    pub result: Result<T, ToolError>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticatedSession {
    pub session_id: SessionId,
    pub actor_id: ActorId,
    pub role: Role,
}
