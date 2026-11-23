//! Transport-independent coordination operations.

pub mod human;
pub mod reviews;
pub mod sessions;
pub mod tasks;

use crate::core::*;

pub trait Coordination: Send + Sync {
    fn list_projects(&self, ctx: &SessionContext, page: PageRequest) -> CoreResult<Page<Project>>;
    fn list_runs(
        &self,
        ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Run>>;
    fn list_tasks(
        &self,
        ctx: &SessionContext,
        run: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>>;
    fn create_task(
        &self,
        ctx: &SessionContext,
        request: CreateTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn assign_task(
        &self,
        ctx: &SessionContext,
        request: AssignTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn update_progress(
        &self,
        ctx: &SessionContext,
        request: UpdateProgress,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn send_message(
        &self,
        ctx: &SessionContext,
        request: SendMessage,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Message>>;
    fn request_workspace(
        &self,
        ctx: &SessionContext,
        request: WorkspaceRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>>;
    fn request_review(
        &self,
        ctx: &SessionContext,
        request: ReviewRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<ReviewRound>>;
    fn submit_review(
        &self,
        ctx: &SessionContext,
        request: SubmitReview,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Review>>;
    fn request_human(
        &self,
        ctx: &SessionContext,
        request: HumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>>;
    fn timeline(
        &self,
        ctx: &SessionContext,
        scope: EventScope,
        page: PageRequest,
    ) -> CoreResult<Page<Event>>;
}
