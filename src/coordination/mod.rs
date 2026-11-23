//! Transport-independent coordination operations.

pub mod human;
pub mod reviews;
pub mod sessions;
pub mod tasks;

use crate::core::*;

pub trait Coordination: Send + Sync {
    fn list_projects(&self, ctx: &SessionContext, page: PageRequest) -> CoreResult<Page<Project>>;
    fn list_repositories(
        &self,
        ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Repository>>;
    fn list_agents(
        &self,
        ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Agent>>;
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
    fn available_work(
        &self,
        ctx: &SessionContext,
        project: ProjectId,
        page: PageRequest,
    ) -> CoreResult<Page<Task>>;
    fn create_run(
        &self,
        ctx: &SessionContext,
        request: CreateRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>>;
    fn create_task(
        &self,
        ctx: &SessionContext,
        request: CreateTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn create_task_with_dependencies(
        &self,
        ctx: &SessionContext,
        request: CreateTaskWithDependencies,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn assign_task(
        &self,
        ctx: &SessionContext,
        request: AssignTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn claim_task(
        &self,
        ctx: &SessionContext,
        request: ClaimTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn release_task(
        &self,
        ctx: &SessionContext,
        request: ReleaseTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn update_progress(
        &self,
        ctx: &SessionContext,
        request: UpdateProgress,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn submit_task(
        &self,
        ctx: &SessionContext,
        request: SubmitTask,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn record_task_disposition(
        &self,
        ctx: &SessionContext,
        request: FinalTaskDisposition,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn complete_run(
        &self,
        ctx: &SessionContext,
        request: CompleteRun,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Run>>;
    fn send_message(
        &self,
        ctx: &SessionContext,
        request: SendMessage,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Message>>;
    fn read_messages(
        &self,
        ctx: &SessionContext,
        request: ReadMessages,
    ) -> CoreResult<Page<Message>>;
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
    fn list_reviews(&self, ctx: &SessionContext, request: ListReviews) -> CoreResult<Page<Review>>;
    fn disposition_findings(
        &self,
        ctx: &SessionContext,
        request: DispositionFindings,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Task>>;
    fn request_human(
        &self,
        ctx: &SessionContext,
        request: HumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>>;
    fn list_human_requests(
        &self,
        ctx: &SessionContext,
        request: ListHumanRequests,
    ) -> CoreResult<Page<HumanDecision>>;
    fn resolve_human(
        &self,
        ctx: &SessionContext,
        request: ResolveHumanRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<HumanDecision>>;
    fn inspect_workspace(
        &self,
        ctx: &SessionContext,
        request: InspectWorkspace,
    ) -> CoreResult<Workspace>;
    fn cleanup_workspace(
        &self,
        ctx: &SessionContext,
        request: CleanupWorkspace,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>>;
    fn timeline(
        &self,
        ctx: &SessionContext,
        scope: EventScope,
        page: PageRequest,
    ) -> CoreResult<Page<Event>>;
    fn locate_artifacts(
        &self,
        ctx: &SessionContext,
        run: RunId,
        page: PageRequest,
    ) -> CoreResult<Page<ArtifactRef>>;
}
