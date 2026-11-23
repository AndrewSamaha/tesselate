//! Versioned MCP adapter contract.

use crate::core::IdempotencyKey;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
pub const TOOL_NAMESPACE: &str = "tesselate.v1";
pub const TOOLS: &[&str] = &[
    "tesselate.v1.projects.list",
    "tesselate.v1.repositories.list",
    "tesselate.v1.agents.list",
    "tesselate.v1.runs.list",
    "tesselate.v1.runs.create",
    "tesselate.v1.work.available",
    "tesselate.v1.tasks.list",
    "tesselate.v1.tasks.create",
    "tesselate.v1.tasks.assign",
    "tesselate.v1.tasks.progress",
    "tesselate.v1.tasks.submit",
    "tesselate.v1.tasks.disposition",
    "tesselate.v1.runs.complete",
    "tesselate.v1.messages.send",
    "tesselate.v1.messages.read",
    "tesselate.v1.workspaces.request",
    "tesselate.v1.reviews.request",
    "tesselate.v1.reviews.submit",
    "tesselate.v1.reviews.list",
    "tesselate.v1.reviews.rounds.list",
    "tesselate.v1.reviews.disposition",
    "tesselate.v1.human.request",
    "tesselate.v1.human.list",
    "tesselate.v1.human.resolve",
    "tesselate.v1.workspaces.inspect",
    "tesselate.v1.workspaces.cleanup",
    "tesselate.v1.timeline.read",
    "tesselate.v1.artifacts.locate",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadRequest<T> {
    pub schema_version: u16,
    pub request_id: String,
    pub params: T,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MutationRequest<T> {
    pub schema_version: u16,
    pub request_id: String,
    pub idempotency_key: IdempotencyKey,
    pub params: T,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResponse<T> {
    pub schema_version: u16,
    pub request_id: String,
    pub outcome: ToolOutcome<T>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "data", rename_all = "snake_case")]
pub enum ToolOutcome<T> {
    Success(T),
    Error(ToolError),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
