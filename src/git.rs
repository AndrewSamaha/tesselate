//! Safe, recoverable local Git workspace operations (F03).

use crate::{
    coordination::{
        reviews::CommitAuthorLookup,
        tasks::{AssignmentRecord, AssignmentStatus, TaskRecord},
    },
    core::*,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use uuid::Uuid;

const PAGE_SIZE: u32 = 500;
const CREATE_OPERATION: &str = "workspaces.request";
const CAPTURE_OPERATION: &str = "workspaces.commits.capture";
const CLEANUP_OPERATION: &str = "workspaces.cleanup";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceCommit {
    pub workspace_id: WorkspaceId,
    pub commit: CommitRef,
}

#[derive(Clone)]
pub struct GitWorkspaceManager {
    db: Database,
    root: PathBuf,
}

impl GitWorkspaceManager {
    /// `root` is a dedicated directory for F03-created worktrees. It is created if missing.
    pub fn new(db: Database, root: impl AsRef<Path>) -> CoreResult<Self> {
        fs::create_dir_all(root.as_ref()).map_err(io_error)?;
        let root = fs::canonicalize(root.as_ref()).map_err(io_error)?;
        Ok(Self { db, root })
    }

    /// Creates or reconciles the workspace for the task's current assignment and repository.
    pub fn request_workspace(
        &self,
        ctx: &SessionContext,
        request: WorkspaceRequest,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, CREATE_OPERATION, &key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        let (run, assignment) = self.authorized_assignment(ctx, request.task_id)?;
        let (repository, _) = self
            .db
            .load::<Repository>(EntityKind::Repository, &request.repository_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        if repository.project_id != run.project_id {
            return Err(CoreError::Invalid(
                "repository is not registered to the task project".into(),
            ));
        }
        let repo = self.validate_repository(&repository)?;
        let base = self.resolve_commit(&repo, &request.base_commit)?;
        let path = self.workspace_path(&assignment.id, request.repository_id);
        let branch = format!("tesselate/{}/{}", assignment.id, request.repository_id.0);
        let (existing, revision) =
            self.find_intent(run.project_id, &assignment.id, request.repository_id)?;
        if let Some(mut intent) = existing {
            if intent.assignment_key != assignment.id
                || intent.repository_id != repository.id
                || intent.canonical_repository_path != repo.to_string_lossy()
                || intent.owner != assignment.assignee
                || intent.base_commit != base
                || intent.worktree_path != path.to_string_lossy()
                || intent.branch != branch
            {
                return Err(CoreError::Recoverable("assignment/repository already has an intent with different workspace parameters".into()));
            }
            if intent.checkpoint == GitCheckpoint::Conflict {
                return Err(CoreError::Recoverable(
                    intent
                        .recovery_reason
                        .unwrap_or_else(|| "Git intent is conflicted".into()),
                ));
            }
            if intent.checkpoint == GitCheckpoint::Cleaned {
                return Err(CoreError::Recoverable(
                    "workspace was explicitly cleaned up for this assignment".into(),
                ));
            }
            if intent.checkpoint == GitCheckpoint::Recorded {
                let workspace = intent.result.clone().ok_or_else(|| {
                    CoreError::Internal("recorded Git intent has no workspace".into())
                })?;
                self.ensure_workspace_intent(&workspace, &intent)?;
                match self.observe_optional(&repo, &path) {
                    Ok(Some(observed)) if self.observation_matches(&intent, &observed) => {}
                    Ok(Some(observed)) => {
                        return self.mark_conflict(
                            ctx,
                            &run,
                            request.task_id,
                            intent,
                            revision,
                            observed,
                            "recorded workspace no longer matches its Git intent",
                        )
                    }
                    Ok(None) => {
                        return self.mark_conflict(
                            ctx,
                            &run,
                            request.task_id,
                            intent,
                            revision,
                            self.unknown_observation(&repo, &path),
                            "recorded workspace is missing",
                        )
                    }
                    Err(error) => {
                        return self.mark_conflict(
                            ctx,
                            &run,
                            request.task_id,
                            intent,
                            revision,
                            self.unknown_observation(&repo, &path),
                            &format!("recorded workspace cannot be inspected: {error}"),
                        )
                    }
                }
                return self.record_reuse(
                    ctx,
                    &intent,
                    &request,
                    &key,
                    &workspace,
                    CREATE_OPERATION,
                    "workspace.request_reused",
                );
            }
            if intent.checkpoint == GitCheckpoint::CleanupStarted {
                return Err(CoreError::Recoverable(
                    "workspace cleanup is incomplete; inspect before retrying creation".into(),
                ));
            }
            self.reconcile_create(ctx, &mut intent, revision, &request, &key)
        } else {
            let id = intent_id(&assignment.id, repository.id);
            let intent = GitOperationIntent {
                id,
                assignment_key: assignment.id,
                repository_id: repository.id,
                canonical_repository_path: repo.to_string_lossy().into_owned(),
                base_commit: base,
                branch,
                worktree_path: path.to_string_lossy().into_owned(),
                owner: assignment.assignee,
                checkpoint: GitCheckpoint::Prepared,
                observed: None,
                result: None,
                recovery_reason: None,
            };
            let event = self.event(
                ctx,
                &run,
                request.task_id,
                "workspace.intent_prepared",
                json!({"intent_id":id,"repository_id":repository.id}),
            );
            self.db
                .commit_mutation(MutationInput {
                    context: ctx,
                    operation: "git.intents.prepare",
                    idempotency_key: &IdempotencyKey(format!("git-intent-{}-prepared", id.0)),
                    request: &intent,
                    preconditions: vec![],
                    result: &intent,
                    changes: vec![StateChange::insert(
                        EntityKind::GitOperation,
                        id.0.to_string(),
                        intent_scope(run.project_id, &intent),
                        &intent,
                    )
                    .map_err(storage_error)?],
                    event,
                })
                .map_err(storage_error)?;
            let (mut persisted, revision) = self.load_intent(&intent)?;
            if persisted.checkpoint == GitCheckpoint::Recorded {
                let workspace = persisted.result.clone().ok_or_else(|| {
                    CoreError::Internal("recorded Git intent has no workspace".into())
                })?;
                return self.record_reuse(
                    ctx,
                    &persisted,
                    &request,
                    &key,
                    &workspace,
                    CREATE_OPERATION,
                    "workspace.request_reused",
                );
            }
            self.reconcile_create(ctx, &mut persisted, revision, &request, &key)
        }
    }

    /// Read-only inspection of the current worktree HEAD and working-tree status.
    pub fn inspect_workspace(
        &self,
        ctx: &SessionContext,
        request: InspectWorkspace,
    ) -> CoreResult<WorkspaceInspection> {
        let (workspace, _) = self
            .db
            .load::<Workspace>(EntityKind::Workspace, &request.workspace_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        self.authorize_workspace_read(ctx, &workspace)?;
        let repo = self.repository_path(workspace.repository_id)?;
        let path = self.checked_workspace_path(&workspace.path)?;
        let observed = self.observe(&repo, &path)?;
        let (intent, _) = self.intent_for_workspace(&workspace)?;
        self.ensure_workspace_intent(&workspace, &intent)?;
        if intent.checkpoint != GitCheckpoint::Recorded
            || !self.observation_matches(&intent, &observed)
        {
            return Err(CoreError::Recoverable(
                "workspace does not match its recorded repository, path, or branch".into(),
            ));
        }
        Ok(WorkspaceInspection {
            head: CommitRef {
                repository_id: workspace.repository_id,
                commit_id: observed
                    .head_commit
                    .clone()
                    .ok_or_else(|| CoreError::Recoverable("worktree HEAD is unavailable".into()))?,
            },
            clean: observed.clean.unwrap_or(false),
            changed_paths: self.changed_paths(&path)?,
            workspace,
        })
    }

    /// Persist all commits in `base..expected_head`; the expected head makes retries stable.
    pub fn capture_commit_refs(
        &self,
        ctx: &SessionContext,
        request: CaptureWorkspaceCommits,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Vec<CommitRef>>> {
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, CAPTURE_OPERATION, &key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        let (workspace, workspace_revision) = self
            .db
            .load::<Workspace>(EntityKind::Workspace, &request.workspace_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        self.authorize_workspace_read(ctx, &workspace)?;
        let repo = self.repository_path(workspace.repository_id)?;
        let path = self.checked_workspace_path(&workspace.path)?;
        let observed = self.observe(&repo, &path)?;
        let (intent, _) = self.intent_for_workspace(&workspace)?;
        self.ensure_workspace_intent(&workspace, &intent)?;
        if intent.checkpoint != GitCheckpoint::Recorded
            || !self.observation_matches(&intent, &observed)
        {
            return Err(CoreError::Recoverable(
                "workspace does not match its recorded repository, path, or branch".into(),
            ));
        }
        let expected = self.resolve_commit(&repo, &request.expected_head)?;
        if observed.head_commit.as_deref() != Some(expected.as_str()) {
            return Err(CoreError::Recoverable(
                "workspace HEAD changed; inspect and retry with the observed head".into(),
            ));
        }
        self.ensure_ancestor(&repo, &workspace.base_commit, &expected)?;
        let output = self.git(
            &repo,
            [
                "rev-list",
                "--reverse",
                &format!("{}..{}", workspace.base_commit, expected),
            ],
        )?;
        let commits = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .map(|commit_id| CommitRef {
                repository_id: workspace.repository_id,
                commit_id: commit_id.into(),
            })
            .collect::<Vec<_>>();
        let mut changes = vec![StateChange::update(
            EntityKind::Workspace,
            workspace.id.0.to_string(),
            self.workspace_scope(&workspace)?,
            workspace_revision,
            &workspace,
        )
        .map_err(storage_error)?];
        for commit in &commits {
            let id = format!("{}:{}", workspace.id.0, commit.commit_id);
            match self
                .db
                .load::<WorkspaceCommit>(EntityKind::WorkspaceCommit, &id)
                .map_err(storage_error)?
            {
                Some((existing, revision)) => {
                    if existing.commit != *commit {
                        return Err(CoreError::Recoverable(
                            "stored workspace commit identity conflicts with Git".into(),
                        ));
                    }
                    changes.push(
                        StateChange::update(
                            EntityKind::WorkspaceCommit,
                            id,
                            self.workspace_scope(&workspace)?,
                            revision,
                            &existing,
                        )
                        .map_err(storage_error)?,
                    );
                }
                None => changes.push(
                    StateChange::insert(
                        EntityKind::WorkspaceCommit,
                        id,
                        self.workspace_scope(&workspace)?,
                        &WorkspaceCommit {
                            workspace_id: workspace.id,
                            commit: commit.clone(),
                        },
                    )
                    .map_err(storage_error)?,
                ),
            }
        }
        let run = self.task_run(workspace.task_id)?;
        let event = self.event(
            ctx,
            &run,
            workspace.task_id,
            "workspace.commits_captured",
            json!({"workspace_id":workspace.id,"head":expected,"commit_count":commits.len()}),
        );
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: CAPTURE_OPERATION,
                idempotency_key: &key,
                request: &request,
                preconditions: vec![],
                result: &commits,
                changes,
                event,
            })
            .map_err(storage_error)
    }

    /// Explicitly removes a clean worktree and its branch; dirty worktrees are never forced.
    pub fn cleanup_workspace(
        &self,
        ctx: &SessionContext,
        request: CleanupWorkspace,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        if let Some(prior) = self
            .db
            .prior_mutation(ctx, CLEANUP_OPERATION, &key, &request)
            .map_err(storage_error)?
        {
            return Ok(prior);
        }
        let (workspace, _) = self
            .db
            .load::<Workspace>(EntityKind::Workspace, &request.workspace_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        self.authorize_workspace_cleanup(ctx, &workspace)?;
        let run = self.task_run(workspace.task_id)?;
        let (mut intent, revision) = self.intent_for_workspace(&workspace)?;
        self.ensure_workspace_intent(&workspace, &intent)?;
        if intent.checkpoint == GitCheckpoint::Cleaned {
            return self.record_reuse(
                ctx,
                &intent,
                &request,
                &key,
                &workspace,
                CLEANUP_OPERATION,
                "workspace.cleanup_replayed",
            );
        }
        if intent.checkpoint == GitCheckpoint::Conflict {
            return Err(CoreError::Recoverable(
                intent
                    .recovery_reason
                    .unwrap_or_else(|| "Git intent is conflicted".into()),
            ));
        }
        let repo = self.repository_path(workspace.repository_id)?;
        let path = self.checked_workspace_path(&workspace.path)?;
        let observed_state = match self.observe_optional(&repo, &path) {
            Ok(observed) => observed,
            Err(error) => {
                return self.mark_conflict(
                    ctx,
                    &run,
                    workspace.task_id,
                    intent,
                    revision,
                    self.unknown_observation(&repo, &path),
                    &format!("recorded worktree cannot be inspected: {error}"),
                )
            }
        };
        match observed_state {
            Some(observed) => {
                if !self.observation_matches(&intent, &observed) {
                    return self.mark_conflict(
                        ctx,
                        &run,
                        workspace.task_id,
                        intent,
                        revision,
                        observed,
                        "worktree branch differs from recorded branch",
                    );
                }
                if !observed.clean.unwrap_or(false) {
                    return Err(CoreError::Recoverable(
                        "worktree has uncommitted changes; cleanup refused".into(),
                    ));
                }
            }
            None => {
                let branch_exists = self.branch_exists(&repo, &workspace.branch)?;
                if intent.checkpoint == GitCheckpoint::CleanupStarted && !branch_exists {
                    intent.checkpoint = GitCheckpoint::Cleaned;
                    intent.observed = None;
                    intent.recovery_reason = None;
                    return self.save_workspace_checkpoint(
                        ctx,
                        &run,
                        workspace.task_id,
                        intent,
                        revision,
                        &request,
                        &key,
                        &workspace,
                        CLEANUP_OPERATION,
                        "workspace.cleaned",
                    );
                }
                let observed = ObservedGitState {
                    repository_path: repo.to_string_lossy().into_owned(),
                    worktree_path: workspace.path.clone(),
                    branch: branch_exists.then(|| workspace.branch.clone()),
                    head_commit: None,
                    clean: None,
                };
                return self.mark_conflict(
                    ctx,
                    &run,
                    workspace.task_id,
                    intent,
                    revision,
                    observed,
                    "recorded worktree or branch is missing outside a reconciled cleanup",
                );
            }
        }
        if intent.checkpoint != GitCheckpoint::CleanupStarted {
            intent.checkpoint = GitCheckpoint::CleanupStarted;
            intent.recovery_reason = None;
            let started = self.save_intent_step(
                ctx,
                &run,
                workspace.task_id,
                intent,
                revision,
                "workspace.cleanup_started",
                json!({"workspace_id":workspace.id}),
            )?;
            intent = started.0;
        }
        self.git(&repo, ["worktree", "remove", "--", &workspace.path])?;
        self.git(&repo, ["branch", "-D", "--", &workspace.branch])?;
        intent.checkpoint = GitCheckpoint::Cleaned;
        intent.observed = None;
        intent.recovery_reason = None;
        let (_, new_revision) = self.load_intent(&intent)?;
        self.save_workspace_checkpoint(
            ctx,
            &run,
            workspace.task_id,
            intent,
            new_revision,
            &request,
            &key,
            &workspace,
            CLEANUP_OPERATION,
            "workspace.cleaned",
        )
    }

    fn reconcile_create(
        &self,
        ctx: &SessionContext,
        intent: &mut GitOperationIntent,
        mut revision: i64,
        request: &WorkspaceRequest,
        key: &IdempotencyKey,
    ) -> CoreResult<Mutation<Workspace>> {
        let run = self.task_run(request.task_id)?;
        if intent.checkpoint == GitCheckpoint::Prepared {
            intent.checkpoint = GitCheckpoint::MutationStarted;
            let (updated, new_revision) = self.save_intent_step(
                ctx,
                &run,
                request.task_id,
                intent.clone(),
                revision,
                "workspace.mutation_started",
                json!({"intent_id":intent.id}),
            )?;
            *intent = updated;
            revision = new_revision;
        }
        let repo = PathBuf::from(&intent.canonical_repository_path);
        let path = PathBuf::from(&intent.worktree_path);
        let observed = match self.observe_optional(&repo, &path) {
            Ok(observed) => observed,
            Err(error) => {
                return self.mark_conflict(
                    ctx,
                    &run,
                    request.task_id,
                    intent.clone(),
                    revision,
                    self.unknown_observation(&repo, &path),
                    &format!("partial worktree cannot be inspected: {error}"),
                )
            }
        };
        match observed {
            Some(observed) if self.observation_matches(intent, &observed) => {
                if intent.checkpoint == GitCheckpoint::MutationStarted {
                    intent.checkpoint = GitCheckpoint::GitApplied;
                    intent.observed = Some(observed.clone());
                    let (updated, new_revision) = self.save_intent_step(
                        ctx,
                        &run,
                        request.task_id,
                        intent.clone(),
                        revision,
                        "workspace.git_applied",
                        json!({"intent_id":intent.id,"head":observed.head_commit}),
                    )?;
                    *intent = updated;
                    revision = new_revision;
                }
                if observed.head_commit.as_deref() != Some(intent.base_commit.as_str())
                    || observed.clean != Some(true)
                {
                    return self.mark_conflict(
                        ctx,
                        &run,
                        request.task_id,
                        intent.clone(),
                        revision,
                        observed,
                        "new workspace does not match its requested base state",
                    );
                }
            }
            Some(observed) => {
                return self.mark_conflict(
                    ctx,
                    &run,
                    request.task_id,
                    intent.clone(),
                    revision,
                    observed,
                    "path exists but does not match the persisted Git intent",
                )
            }
            None => {
                if self.branch_exists(&repo, &intent.branch)? || path_entry_exists(&path) {
                    return self.mark_conflict(
                        ctx,
                        &run,
                        request.task_id,
                        intent.clone(),
                        revision,
                        ObservedGitState {
                            repository_path: repo.to_string_lossy().into_owned(),
                            worktree_path: path.to_string_lossy().into_owned(),
                            branch: None,
                            head_commit: None,
                            clean: None,
                        },
                        "branch or worktree path exists without a matching worktree",
                    );
                }
                self.git(
                    &repo,
                    [
                        "worktree",
                        "add",
                        "-b",
                        &intent.branch,
                        "--",
                        &intent.worktree_path,
                        &intent.base_commit,
                    ],
                )?;
                let observed = self.observe(&repo, &path)?;
                if !self.observation_matches(intent, &observed)
                    || observed.head_commit.as_deref() != Some(intent.base_commit.as_str())
                    || observed.clean != Some(true)
                {
                    return self.mark_conflict(
                        ctx,
                        &run,
                        request.task_id,
                        intent.clone(),
                        revision,
                        observed,
                        "Git created a worktree that failed postcondition checks",
                    );
                }
                intent.checkpoint = GitCheckpoint::GitApplied;
                intent.observed = Some(observed.clone());
                let (updated, new_revision) = self.save_intent_step(
                    ctx,
                    &run,
                    request.task_id,
                    intent.clone(),
                    revision,
                    "workspace.git_applied",
                    json!({"intent_id":intent.id,"head":observed.head_commit}),
                )?;
                *intent = updated;
                revision = new_revision;
            }
        }
        let workspace = Workspace {
            id: WorkspaceId::new(),
            assignment_id: intent.assignment_key.clone(),
            task_id: request.task_id,
            repository_id: intent.repository_id,
            base_commit: intent.base_commit.clone(),
            branch: intent.branch.clone(),
            path: intent.worktree_path.clone(),
            owner: intent.owner,
        };
        intent.checkpoint = GitCheckpoint::Recorded;
        intent.result = Some(workspace.clone());
        intent.recovery_reason = None;
        let change = StateChange::insert(
            EntityKind::Workspace,
            workspace.id.0.to_string(),
            self.workspace_scope(&workspace)?,
            &workspace,
        )
        .map_err(storage_error)?;
        self.db.commit_mutation(MutationInput { context: ctx, operation: CREATE_OPERATION, idempotency_key: key, request, preconditions: vec![], result: &workspace, changes: vec![StateChange::update(EntityKind::GitOperation, intent.id.0.to_string(), intent_scope(run.project_id, intent), revision, intent).map_err(storage_error)?, change], event: self.event(ctx, &run, request.task_id, "workspace.recorded", json!({"workspace_id":workspace.id,"repository_id":workspace.repository_id,"base_commit":workspace.base_commit,"branch":workspace.branch})) }).map_err(storage_error)
    }

    fn record_reuse<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        intent: &GitOperationIntent,
        request: &Q,
        key: &IdempotencyKey,
        workspace: &Workspace,
        operation: &str,
        event_type: &str,
    ) -> CoreResult<Mutation<Workspace>> {
        let run = self.task_run(workspace.task_id)?;
        let mut updated = intent.clone();
        updated.observed = self.observe_optional(
            Path::new(&updated.canonical_repository_path),
            Path::new(&updated.worktree_path),
        )?;
        let new_revision = self.load_intent(&updated)?.1;
        let mut changes = vec![StateChange::update(
            EntityKind::GitOperation,
            updated.id.0.to_string(),
            intent_scope(run.project_id, &updated),
            new_revision,
            &updated,
        )
        .map_err(storage_error)?];
        let workspace_revision = self
            .db
            .load::<Workspace>(EntityKind::Workspace, &workspace.id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?
            .1;
        changes.push(
            StateChange::update(
                EntityKind::Workspace,
                workspace.id.0.to_string(),
                self.workspace_scope(workspace)?,
                workspace_revision,
                workspace,
            )
            .map_err(storage_error)?,
        );
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation,
                idempotency_key: key,
                request,
                preconditions: vec![],
                result: workspace,
                changes,
                event: self.event(
                    ctx,
                    &run,
                    workspace.task_id,
                    event_type,
                    json!({"workspace_id":workspace.id}),
                ),
            })
            .map_err(storage_error)
    }

    fn save_workspace_checkpoint<Q: Serialize>(
        &self,
        ctx: &SessionContext,
        run: &Run,
        task_id: TaskId,
        intent: GitOperationIntent,
        revision: i64,
        request: &Q,
        key: &IdempotencyKey,
        workspace: &Workspace,
        operation: &str,
        event_type: &str,
    ) -> CoreResult<Mutation<Workspace>> {
        let change = StateChange::update(
            EntityKind::GitOperation,
            intent.id.0.to_string(),
            intent_scope(run.project_id, &intent),
            revision,
            &intent,
        )
        .map_err(storage_error)?;
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation,
                idempotency_key: key,
                request,
                preconditions: vec![],
                result: workspace,
                changes: vec![change],
                event: self.event(
                    ctx,
                    run,
                    task_id,
                    event_type,
                    json!({"workspace_id":workspace.id,"intent_id":intent.id}),
                ),
            })
            .map_err(storage_error)
    }

    fn save_intent_step(
        &self,
        ctx: &SessionContext,
        run: &Run,
        task_id: TaskId,
        intent: GitOperationIntent,
        revision: i64,
        event_type: &str,
        payload: serde_json::Value,
    ) -> CoreResult<(GitOperationIntent, i64)> {
        let key = IdempotencyKey(format!(
            "git-intent-{}-{}",
            intent.id.0,
            checkpoint_name(intent.checkpoint)
        ));
        let change = StateChange::update(
            EntityKind::GitOperation,
            intent.id.0.to_string(),
            intent_scope(run.project_id, &intent),
            revision,
            &intent,
        )
        .map_err(storage_error)?;
        let result = self
            .db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: "git.intents.checkpoint",
                idempotency_key: &key,
                request: &intent,
                preconditions: vec![],
                result: &intent,
                changes: vec![change],
                event: self.event(ctx, run, task_id, event_type, payload),
            })
            .map_err(storage_error)?;
        let (_, revision) = self.load_intent(&result.value)?;
        Ok((result.value, revision))
    }

    fn mark_conflict(
        &self,
        ctx: &SessionContext,
        run: &Run,
        task_id: TaskId,
        mut intent: GitOperationIntent,
        revision: i64,
        observed: ObservedGitState,
        reason: &str,
    ) -> CoreResult<Mutation<Workspace>> {
        intent.checkpoint = GitCheckpoint::Conflict;
        intent.observed = Some(observed);
        intent.recovery_reason = Some(reason.into());
        let key = IdempotencyKey(format!("git-intent-{}-conflict", intent.id.0));
        let change = StateChange::update(
            EntityKind::GitOperation,
            intent.id.0.to_string(),
            intent_scope(run.project_id, &intent),
            revision,
            &intent,
        )
        .map_err(storage_error)?;
        self.db
            .commit_mutation(MutationInput {
                context: ctx,
                operation: "git.intents.conflict",
                idempotency_key: &key,
                request: &intent,
                preconditions: vec![],
                result: &intent,
                changes: vec![change],
                event: self.event(
                    ctx,
                    run,
                    task_id,
                    "workspace.conflict",
                    json!({"intent_id":intent.id,"reason":reason}),
                ),
            })
            .map_err(storage_error)?;
        Err(CoreError::Recoverable(reason.into()))
    }

    fn authorized_assignment(
        &self,
        ctx: &SessionContext,
        task_id: TaskId,
    ) -> CoreResult<(Run, AssignmentRecord)> {
        if !matches!(ctx.role(), Role::Coder | Role::Supervisor) {
            return Err(CoreError::Forbidden);
        }
        let (task, _) = self
            .db
            .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        if !matches!(task.task.status, TaskStatus::Assigned | TaskStatus::Coding) {
            return Err(CoreError::Recoverable(
                "task has no active workspace-eligible assignment".into(),
            ));
        }
        let (run, _) = self
            .db
            .load::<Run>(EntityKind::Run, &task.task.run_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        let scope = EntityScope {
            run_id: Some(run.id.0.to_string()),
            task_id: Some(task_id.0.to_string()),
            ..Default::default()
        };
        let mut after = None;
        let assignment = loop {
            let page = self
                .db
                .list_entities::<AssignmentRecord>(
                    EntityKind::Assignment,
                    &scope,
                    PageRequest {
                        limit: PAGE_SIZE,
                        after,
                    },
                )
                .map_err(storage_error)?;
            let count = page.items.len();
            if let Some(assignment) = page.items.into_iter().find(|assignment| {
                assignment.status == AssignmentStatus::Active
                    && task.task.owner == Some(assignment.assignee)
            }) {
                break assignment;
            }
            if count < PAGE_SIZE as usize {
                return Err(CoreError::Recoverable(
                    "task has no current active assignment".into(),
                ));
            }
            after = page.next_cursor.map(EventCursor);
        };
        if Uuid::parse_str(&assignment.id).is_err() {
            return Err(CoreError::Recoverable(
                "active assignment has a malformed durable ID".into(),
            ));
        }
        if ctx.role() == Role::Coder && ctx.actor_id() != assignment.assignee {
            return Err(CoreError::Forbidden);
        }
        Ok((run, assignment))
    }

    fn authorize_workspace_read(
        &self,
        ctx: &SessionContext,
        workspace: &Workspace,
    ) -> CoreResult<()> {
        match ctx.role() {
            Role::Supervisor => Ok(()),
            Role::Coder if ctx.actor_id() == workspace.owner => Ok(()),
            Role::Reviewer => {
                let (task, _) = self
                    .db
                    .load::<TaskRecord>(EntityKind::Task, &workspace.task_id.0.to_string())
                    .map_err(storage_error)?
                    .ok_or(CoreError::NotFound)?;
                if task.task.status == TaskStatus::InReview {
                    Ok(())
                } else {
                    Err(CoreError::Forbidden)
                }
            }
            _ => Err(CoreError::Forbidden),
        }
    }

    fn authorize_workspace_cleanup(
        &self,
        ctx: &SessionContext,
        workspace: &Workspace,
    ) -> CoreResult<()> {
        if ctx.role() == Role::Supervisor
            || (ctx.role() == Role::Coder && ctx.actor_id() == workspace.owner)
        {
            Ok(())
        } else {
            Err(CoreError::Forbidden)
        }
    }

    fn validate_repository(&self, repository: &Repository) -> CoreResult<PathBuf> {
        let configured = PathBuf::from(&repository.canonical_path);
        let registered = fs::canonicalize(&configured).map_err(io_error)?;
        if self.root.starts_with(&registered) {
            return Err(CoreError::Invalid(
                "configured worktree root must be outside the registered repository".into(),
            ));
        }
        if registered != configured {
            return Err(CoreError::Invalid(
                "registered repository path no longer resolves to its canonical location".into(),
            ));
        }
        let top = self.git_output(&registered, ["rev-parse", "--show-toplevel"])?;
        let actual =
            fs::canonicalize(String::from_utf8_lossy(&top.stdout).trim()).map_err(io_error)?;
        if actual != registered || !registered.is_dir() {
            return Err(CoreError::Invalid(
                "registered path is not the canonical root of a local Git repository".into(),
            ));
        }
        Ok(registered)
    }

    fn repository_path(&self, repository_id: RepositoryId) -> CoreResult<PathBuf> {
        let (repository, _) = self
            .db
            .load::<Repository>(EntityKind::Repository, &repository_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        self.validate_repository(&repository)
    }

    fn resolve_commit(&self, repo: &Path, commit: &str) -> CoreResult<String> {
        if commit.trim().is_empty() || commit.starts_with('-') {
            return Err(CoreError::Invalid(
                "base/head must be a Git commit ID".into(),
            ));
        }
        let output = self.git(
            repo,
            ["rev-parse", "--verify", &format!("{}^{{commit}}", commit)],
        )?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    fn ensure_ancestor(&self, repo: &Path, base: &str, head: &str) -> CoreResult<()> {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["merge-base", "--is-ancestor", base, head])
            .output()
            .map_err(io_error)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(CoreError::Invalid(
                "observed head is not descended from the recorded base commit".into(),
            ))
        }
    }

    fn workspace_path(&self, assignment_id: &str, repository_id: RepositoryId) -> PathBuf {
        self.root
            .join(format!("{}-{}", assignment_id, repository_id.0))
    }

    fn checked_workspace_path(&self, path: &str) -> CoreResult<PathBuf> {
        let path = PathBuf::from(path);
        if path.parent() != Some(self.root.as_path())
            || path.file_name().is_none()
            || path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(CoreError::Invalid(
                "workspace path escapes the configured worktree root".into(),
            ));
        }
        Ok(path)
    }

    fn unknown_observation(&self, repo: &Path, path: &Path) -> ObservedGitState {
        ObservedGitState {
            repository_path: repo.to_string_lossy().into_owned(),
            worktree_path: path.to_string_lossy().into_owned(),
            branch: None,
            head_commit: None,
            clean: None,
        }
    }

    fn observe_optional(&self, repo: &Path, path: &Path) -> CoreResult<Option<ObservedGitState>> {
        if path_entry_exists(path) {
            self.observe(repo, path).map(Some)
        } else {
            Ok(None)
        }
    }

    fn observe(&self, repo: &Path, path: &Path) -> CoreResult<ObservedGitState> {
        let top_output = self.git_output(path, ["rev-parse", "--show-toplevel"])?;
        let actual_top = fs::canonicalize(String::from_utf8_lossy(&top_output.stdout).trim())
            .map_err(io_error)?;
        let expected_path = fs::canonicalize(path).map_err(io_error)?;
        let expected_repo = fs::canonicalize(repo).map_err(io_error)?;
        let actual_common_output = self.git_output(
            path,
            ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let expected_common_output = self.git_output(
            repo,
            ["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let actual_common =
            fs::canonicalize(String::from_utf8_lossy(&actual_common_output.stdout).trim())
                .map_err(io_error)?;
        let expected_common =
            fs::canonicalize(String::from_utf8_lossy(&expected_common_output.stdout).trim())
                .map_err(io_error)?;
        let head = self.git(path, ["rev-parse", "--verify", "HEAD^{commit}"])?;
        let branch_output =
            self.git_output(path, ["symbolic-ref", "--quiet", "--short", "HEAD"])?;
        let branch = String::from_utf8_lossy(&branch_output.stdout)
            .trim()
            .to_owned();
        let status = self.git(
            path,
            [
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--ignored=traditional",
            ],
        )?;
        let repository_path = if actual_common == expected_common {
            expected_repo.to_string_lossy().into_owned()
        } else {
            actual_top.to_string_lossy().into_owned()
        };
        Ok(ObservedGitState {
            repository_path,
            worktree_path: expected_path.to_string_lossy().into_owned(),
            branch: Some(branch),
            head_commit: Some(String::from_utf8_lossy(&head.stdout).trim().into()),
            clean: Some(
                status.stdout.is_empty()
                    && actual_top == expected_path
                    && actual_common == expected_common,
            ),
        })
    }

    fn observation_matches(
        &self,
        intent: &GitOperationIntent,
        observed: &ObservedGitState,
    ) -> bool {
        observed.repository_path == intent.canonical_repository_path
            && observed.worktree_path == intent.worktree_path
            && observed.branch.as_deref() == Some(intent.branch.as_str())
    }

    fn changed_paths(&self, path: &Path) -> CoreResult<Vec<String>> {
        let output = self.git(
            path,
            [
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=all",
                "--ignored=traditional",
            ],
        )?;
        let records = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|record| !record.is_empty())
            .collect::<Vec<_>>();
        let mut paths = Vec::new();
        let mut index = 0;
        while index < records.len() {
            let record = records[index];
            if record.len() >= 4 {
                paths.push(String::from_utf8_lossy(&record[3..]).into_owned());
            }
            if record
                .get(..2)
                .is_some_and(|status| status.contains(&b'R') || status.contains(&b'C'))
            {
                index += 1;
                if let Some(old_path) = records.get(index) {
                    paths.push(String::from_utf8_lossy(old_path).into_owned());
                }
            }
            index += 1;
        }
        Ok(paths)
    }

    fn branch_exists(&self, repo: &Path, branch: &str) -> CoreResult<bool> {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args([
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ])
            .output()
            .map_err(io_error)?;
        if output.status.success() {
            Ok(true)
        } else if output.status.code() == Some(1) {
            Ok(false)
        } else {
            Err(CoreError::Recoverable(
                String::from_utf8_lossy(&output.stderr).trim().into(),
            ))
        }
    }

    fn git<I, S>(&self, repo: &Path, args: I) -> CoreResult<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .map_err(io_error)?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(CoreError::Recoverable(format!(
                "git command failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )))
        }
    }

    fn git_output<I, S>(&self, repo: &Path, args: I) -> CoreResult<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        self.git(repo, args)
    }

    fn find_intent(
        &self,
        project_id: ProjectId,
        assignment_id: &str,
        repository_id: RepositoryId,
    ) -> CoreResult<(Option<GitOperationIntent>, i64)> {
        let mut after = None;
        loop {
            let page = self
                .db
                .list_entities::<GitOperationIntent>(
                    EntityKind::GitOperation,
                    &EntityScope {
                        project_id: Some(project_id.0.to_string()),
                        ..Default::default()
                    },
                    PageRequest {
                        limit: PAGE_SIZE,
                        after,
                    },
                )
                .map_err(storage_error)?;
            let count = page.items.len();
            for intent in page.items {
                if intent.assignment_key == assignment_id && intent.repository_id == repository_id {
                    let revision = self
                        .db
                        .load::<GitOperationIntent>(
                            EntityKind::GitOperation,
                            &intent.id.0.to_string(),
                        )
                        .map_err(storage_error)?
                        .ok_or(CoreError::NotFound)?
                        .1;
                    return Ok((Some(intent), revision));
                }
            }
            if count < PAGE_SIZE as usize {
                return Ok((None, 0));
            }
            after = page.next_cursor.map(EventCursor);
        }
    }

    fn intent_for_workspace(&self, workspace: &Workspace) -> CoreResult<(GitOperationIntent, i64)> {
        let (intent, revision) = self.find_intent(
            self.task_run(workspace.task_id)?.project_id,
            &workspace.assignment_id,
            workspace.repository_id,
        )?;
        intent
            .map(|intent| (intent, revision))
            .ok_or(CoreError::NotFound)
    }

    fn load_intent(&self, intent: &GitOperationIntent) -> CoreResult<(GitOperationIntent, i64)> {
        self.db
            .load(EntityKind::GitOperation, &intent.id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }

    fn task_run(&self, task_id: TaskId) -> CoreResult<Run> {
        let (task, _) = self
            .db
            .load::<TaskRecord>(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?;
        self.db
            .load::<Run>(EntityKind::Run, &task.task.run_id.0.to_string())
            .map_err(storage_error)?
            .map(|(run, _)| run)
            .ok_or(CoreError::NotFound)
    }

    fn ensure_workspace_intent(
        &self,
        workspace: &Workspace,
        intent: &GitOperationIntent,
    ) -> CoreResult<()> {
        if intent.result.as_ref() != Some(workspace)
            || intent.assignment_key != workspace.assignment_id
            || intent.repository_id != workspace.repository_id
            || intent.branch != workspace.branch
            || intent.worktree_path != workspace.path
            || intent.base_commit != workspace.base_commit
            || intent.owner != workspace.owner
        {
            return Err(CoreError::Recoverable(
                "workspace record does not match its durable Git intent".into(),
            ));
        }
        Ok(())
    }

    fn workspace_scope(&self, workspace: &Workspace) -> CoreResult<EntityScope> {
        let run = self.task_run(workspace.task_id)?;
        Ok(EntityScope {
            project_id: Some(run.project_id.0.to_string()),
            run_id: Some(run.id.0.to_string()),
            task_id: Some(workspace.task_id.0.to_string()),
            owner_id: Some(workspace.owner.0.to_string()),
        })
    }

    fn commit_authors(&self, commits: &[CommitRef]) -> CoreResult<HashSet<ActorId>> {
        let requested = commits.iter().cloned().collect::<HashSet<_>>();
        if requested.is_empty() {
            return Ok(HashSet::new());
        }
        let mut found = HashSet::new();
        let mut authors = HashSet::new();
        let mut after = None;
        loop {
            let page = self
                .db
                .list_entities::<WorkspaceCommit>(
                    EntityKind::WorkspaceCommit,
                    &EntityScope::default(),
                    PageRequest {
                        limit: PAGE_SIZE,
                        after,
                    },
                )
                .map_err(storage_error)?;
            let count = page.items.len();
            for recorded in page.items {
                if requested.contains(&recorded.commit) {
                    let workspace = self
                        .db
                        .load::<Workspace>(
                            EntityKind::Workspace,
                            &recorded.workspace_id.0.to_string(),
                        )
                        .map_err(storage_error)?
                        .ok_or(CoreError::NotFound)?
                        .0;
                    if workspace.repository_id != recorded.commit.repository_id {
                        return Err(CoreError::Recoverable(
                            "captured commit does not match its workspace repository".into(),
                        ));
                    }
                    found.insert(recorded.commit);
                    authors.insert(workspace.owner);
                }
            }
            if count < PAGE_SIZE as usize {
                break;
            }
            after = page.next_cursor.map(EventCursor);
        }
        if found != requested {
            return Err(CoreError::Invalid(
                "one or more submitted commits were not captured from a registered workspace"
                    .into(),
            ));
        }
        Ok(authors)
    }

    fn event(
        &self,
        ctx: &SessionContext,
        run: &Run,
        task_id: TaskId,
        kind: &str,
        payload: serde_json::Value,
    ) -> NewEvent {
        NewEvent::from_context(
            ctx,
            ctx.actor_id().0.to_string(),
            EventScope {
                project_id: Some(run.project_id),
                run_id: Some(run.id),
                task_id: Some(task_id),
            },
            kind,
            payload,
        )
    }
}

impl CommitAuthorLookup for GitWorkspaceManager {
    fn authors(&self, commits: &[CommitRef]) -> CoreResult<HashSet<ActorId>> {
        self.commit_authors(commits)
    }
}

fn intent_id(assignment_id: &str, repository_id: RepositoryId) -> GitOperationId {
    // Stable FNV-1a 128 identity lets SQLite serialize concurrent requests for one pair.
    let mut hash = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d_u128;
    for byte in b"tesselate-git-intent\0"
        .iter()
        .chain(assignment_id.as_bytes())
        .chain(repository_id.0.as_bytes())
    {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
    }
    let mut bytes = hash.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    GitOperationId(Uuid::from_bytes(bytes))
}

fn intent_scope(project_id: ProjectId, intent: &GitOperationIntent) -> EntityScope {
    EntityScope {
        project_id: Some(project_id.0.to_string()),
        owner_id: Some(intent.owner.0.to_string()),
        ..Default::default()
    }
}
fn checkpoint_name(checkpoint: GitCheckpoint) -> &'static str {
    match checkpoint {
        GitCheckpoint::Prepared => "prepared",
        GitCheckpoint::MutationStarted => "mutation-started",
        GitCheckpoint::GitApplied => "git-applied",
        GitCheckpoint::Recorded => "recorded",
        GitCheckpoint::CleanupStarted => "cleanup-started",
        GitCheckpoint::Cleaned => "cleaned",
        GitCheckpoint::Conflict => "conflict",
    }
}
fn storage_error(error: PersistenceError) -> CoreError {
    match error {
        PersistenceError::IdempotencyConflict => CoreError::IdempotencyConflict,
        PersistenceError::StateConflict(id) => {
            CoreError::Recoverable(format!("concurrent state change for {id}"))
        }
        other => CoreError::Internal(other.to_string()),
    }
}
fn io_error(error: std::io::Error) -> CoreError {
    CoreError::Recoverable(error.to_string())
}

#[cfg(test)]
mod tests;

fn path_entry_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}
