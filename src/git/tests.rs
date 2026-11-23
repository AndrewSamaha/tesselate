use super::*;
use crate::coordination::tasks::{
    AssignmentLeaseStatus, AssignmentRecord, AssignmentStatus, HumanTransitionGate, LeaseFence,
    ProjectWorkflowPolicy, ReviewCompletionGate, TaskEngine, TaskRecord, TransitionContext,
};
use chrono::Utc;
use serde::de::DeserializeOwned;
use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

struct TestPolicy;
impl ProjectWorkflowPolicy for TestPolicy {
    fn workflow_limits(&self, _: ProjectId) -> CoreResult<WorkflowLimits> {
        Ok(WorkflowLimits {
            max_duration_hours: 24,
            max_concurrent_assignments: 4,
            max_task_retries: 3,
            max_review_rounds: 4,
        })
    }
}
struct TestReviews;
impl ReviewCompletionGate for TestReviews {
    fn ensure_task_complete(&self, _: TaskId) -> CoreResult<Vec<StatePrecondition>> {
        Ok(vec![])
    }
    fn reviewed_commits_for_run(&self, _: RunId) -> CoreResult<Vec<CommitRef>> {
        Ok(vec![])
    }
}
struct TestHuman;
impl HumanTransitionGate for TestHuman {
    fn ensure_allowed(&self, _: &SessionContext, _: TransitionContext) -> CoreResult<()> {
        Ok(())
    }
}
struct TestLeases(Database);
impl AssignmentLeaseStatus for TestLeases {
    fn expired_lease_fence(&self, task_id: TaskId, owner: ActorId) -> CoreResult<LeaseFence> {
        let assignments = self
            .0
            .list_entities::<AssignmentRecord>(
                EntityKind::Assignment,
                &EntityScope {
                    task_id: Some(task_id.0.to_string()),
                    owner_id: Some(owner.0.to_string()),
                    ..Default::default()
                },
                PageRequest {
                    limit: 50,
                    after: None,
                },
            )
            .map_err(|error| CoreError::Internal(error.to_string()))?;
        let assignment = assignments
            .items
            .into_iter()
            .find(|assignment| assignment.status == AssignmentStatus::Active)
            .ok_or(CoreError::NotFound)?;
        let (_, revision) = self
            .0
            .load::<AssignmentRecord>(EntityKind::Assignment, &assignment.id)
            .map_err(|error| CoreError::Internal(error.to_string()))?
            .ok_or(CoreError::NotFound)?;
        Ok(LeaseFence {
            task_id,
            owner_id: owner,
            condition: StatePrecondition {
                kind: EntityKind::Assignment,
                id: assignment.id,
                expected_revision: revision,
            },
        })
    }
}
fn task_engine(db: Database) -> TaskEngine {
    TaskEngine::new(
        db.clone(),
        std::sync::Arc::new(TestPolicy),
        std::sync::Arc::new(TestReviews),
        std::sync::Arc::new(TestHuman),
        std::sync::Arc::new(TestLeases(db)),
    )
}

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("tesselate-f03-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _temp: TempDir,
    db_path: PathBuf,
    db: Database,
    manager: GitWorkspaceManager,
    project_id: ProjectId,
    run_id: RunId,
    task_id: TaskId,
    coder_id: ActorId,
    coder: SessionContext,
    supervisor: SessionContext,
    assignment_id: String,
    repos: Vec<(Repository, String)>,
}

fn fixture(repo_count: usize) -> Fixture {
    let temp = TempDir::new();
    let db_path = temp.path().join("state.sqlite3");
    let db = Database::open(&db_path).unwrap();
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let coder_id = ActorId::new();
    let supervisor_id = ActorId::new();
    let coder = SessionContext::validated(SessionId::new(), coder_id, Role::Coder);
    let supervisor = SessionContext::validated(SessionId::new(), supervisor_id, Role::Supervisor);
    insert(
        &db,
        &supervisor,
        "project",
        EntityKind::Project,
        project_id.0.to_string(),
        EntityScope::default(),
        &Project {
            id: project_id,
            name: "test".into(),
        },
    );
    insert(
        &db,
        &supervisor,
        "coder",
        EntityKind::Actor,
        coder_id.0.to_string(),
        EntityScope::default(),
        &Actor {
            id: coder_id,
            display_name: "coder".into(),
            role: Role::Coder,
        },
    );
    insert(
        &db,
        &supervisor,
        "supervisor",
        EntityKind::Actor,
        supervisor_id.0.to_string(),
        EntityScope::default(),
        &Actor {
            id: supervisor_id,
            display_name: "supervisor".into(),
            role: Role::Supervisor,
        },
    );
    let run = Run {
        id: run_id,
        project_id,
        goal: "test goal".into(),
        started_at: Utc::now(),
        limits: WorkflowLimits {
            max_duration_hours: 4,
            max_concurrent_assignments: 2,
            max_task_retries: 2,
            max_review_rounds: 2,
        },
        status: RunStatus::Active,
        reviewed_commits: vec![],
        outstanding_integration_steps: vec![],
        outcome: None,
    };
    insert(
        &db,
        &supervisor,
        "run",
        EntityKind::Run,
        run_id.0.to_string(),
        EntityScope {
            project_id: Some(project_id.0.to_string()),
            ..Default::default()
        },
        &run,
    );
    let task = Task {
        id: task_id,
        run_id,
        title: "implement".into(),
        goal: "test".into(),
        status: TaskStatus::Assigned,
        owner: Some(coder_id),
        progress: None,
        attempts: 1,
        submitted_commits: vec![],
        result: None,
    };
    insert(
        &db,
        &supervisor,
        "task",
        EntityKind::Task,
        task_id.0.to_string(),
        EntityScope {
            project_id: Some(project_id.0.to_string()),
            run_id: Some(run_id.0.to_string()),
            task_id: Some(task_id.0.to_string()),
            owner_id: Some(coder_id.0.to_string()),
        },
        &TaskRecord {
            task,
            dependencies: vec![],
            waiting_on_dependencies: false,
            final_disposition: None,
        },
    );
    let assignment_id = Uuid::new_v4().to_string();
    let assignment = AssignmentRecord {
        id: assignment_id.clone(),
        project_id,
        run_id,
        task_id,
        assignee: coder_id,
        assigned_by: supervisor_id,
        attempt: 1,
        status: AssignmentStatus::Active,
        assigned_at: Utc::now(),
        ended_reason: None,
    };
    insert(
        &db,
        &supervisor,
        "assignment",
        EntityKind::Assignment,
        assignment_id.clone(),
        EntityScope {
            project_id: Some(project_id.0.to_string()),
            run_id: Some(run_id.0.to_string()),
            task_id: Some(task_id.0.to_string()),
            owner_id: Some(coder_id.0.to_string()),
        },
        &assignment,
    );
    let mut repos = Vec::new();
    for n in 0..repo_count {
        let path = temp.path().join(format!("repo-{n}"));
        fs::create_dir(&path).unwrap();
        git(&path, ["init", "-b", "main"]);
        git(&path, ["config", "user.name", "Test Author"]);
        git(&path, ["config", "user.email", "test@example.invalid"]);
        fs::write(path.join("README.md"), format!("repo {n}\n")).unwrap();
        fs::write(path.join(".gitignore"), "ignored.txt\n").unwrap();
        git(&path, ["add", "README.md", ".gitignore"]);
        git(&path, ["commit", "-m", "initial"]);
        let base = output(&path, ["rev-parse", "HEAD"]);
        let repository = Repository {
            id: RepositoryId::new(),
            project_id,
            name: format!("repo-{n}"),
            canonical_path: fs::canonicalize(&path)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        };
        insert(
            &db,
            &supervisor,
            &format!("repository-{n}"),
            EntityKind::Repository,
            repository.id.0.to_string(),
            EntityScope {
                project_id: Some(project_id.0.to_string()),
                ..Default::default()
            },
            &repository,
        );
        repos.push((repository, base));
    }
    let manager = GitWorkspaceManager::new(db.clone(), temp.path().join("worktrees")).unwrap();
    Fixture {
        _temp: temp,
        db_path,
        db,
        manager,
        project_id,
        run_id,
        task_id,
        coder_id,
        coder,
        supervisor,
        assignment_id,
        repos,
    }
}

fn prepared_intent(f: &Fixture) -> (Run, AssignmentRecord, i64, GitOperationIntent, i64) {
    let (run, assignment, assignment_revision) = f
        .manager
        .authorized_assignment(&f.coder, f.task_id)
        .unwrap();
    let (repository, base_commit) = &f.repos[0];
    let intent = GitOperationIntent {
        id: intent_id(&assignment.id, repository.id),
        assignment_key: assignment.id.clone(),
        repository_id: repository.id,
        canonical_repository_path: repository.canonical_path.clone(),
        base_commit: base_commit.clone(),
        branch: format!("tesselate/{}/{}", assignment.id, repository.id.0),
        worktree_path: f
            .manager
            .workspace_path(&assignment.id, repository.id)
            .to_string_lossy()
            .into_owned(),
        owner: assignment.assignee,
        checkpoint: GitCheckpoint::Prepared,
        observed: None,
        result: None,
        recovery_reason: None,
    };
    f.db.commit_mutation(MutationInput {
        context: &f.supervisor,
        operation: "test.git-intent.prepare",
        idempotency_key: &IdempotencyKey("test-prepared-intent".into()),
        request: &intent,
        preconditions: vec![StatePrecondition {
            kind: EntityKind::Assignment,
            id: assignment.id.clone(),
            expected_revision: assignment_revision,
        }],
        result: &intent,
        changes: vec![StateChange::insert(
            EntityKind::GitOperation,
            intent.id.0.to_string(),
            intent_scope(run.project_id, &intent),
            &intent,
        )
        .unwrap()],
        event: f.manager.event(
            &f.supervisor,
            &run,
            f.task_id,
            "test.git-intent.prepared",
            json!({"intent_id":intent.id}),
        ),
    })
    .unwrap();
    let revision = f.manager.load_intent(&intent).unwrap().1;
    (run, assignment, assignment_revision, intent, revision)
}

fn start_git_mutation(
    f: &Fixture,
    run: &Run,
    assignment: &AssignmentRecord,
    assignment_revision: i64,
    intent: &mut GitOperationIntent,
    revision: i64,
) -> (GitOperationIntent, i64) {
    intent.checkpoint = GitCheckpoint::MutationStarted;
    f.manager
        .save_intent_step(
            &f.coder,
            run,
            f.task_id,
            intent.clone(),
            revision,
            "workspace.mutation_started",
            json!({"intent_id":intent.id}),
            vec![StateChange::update(
                EntityKind::Assignment,
                assignment.id.clone(),
                assignment_scope(assignment),
                assignment_revision,
                assignment,
            )
            .unwrap()],
        )
        .unwrap()
}

#[test]
fn release_before_git_start_fences_stale_prepared_intent() {
    let f = fixture(1);
    let (_run, assignment, assignment_revision, mut intent, intent_revision) = prepared_intent(&f);
    let workspace_request = WorkspaceRequest {
        task_id: f.task_id,
        repository_id: intent.repository_id,
        base_commit: intent.base_commit.clone(),
    };
    task_engine(f.db.clone())
        .release_task(
            &f.supervisor,
            ReleaseTask {
                task_id: f.task_id,
                reason: "release before Git begins".into(),
            },
            IdempotencyKey("release-before-git".into()),
        )
        .unwrap();
    let error = f
        .manager
        .reconcile_create(
            &f.coder,
            &mut intent,
            intent_revision,
            &assignment,
            assignment_revision,
            &workspace_request,
            &IdempotencyKey("create-after-release".into()),
        )
        .unwrap_err();
    assert!(matches!(error, CoreError::Recoverable(_)));
    assert_eq!(
        f.manager.load_intent(&intent).unwrap().0.checkpoint,
        GitCheckpoint::Prepared
    );
    assert!(!PathBuf::from(&intent.worktree_path).exists());
}

#[test]
fn reassignment_before_git_start_fences_stale_prepared_intent() {
    let f = fixture(1);
    let (_run, assignment, assignment_revision, mut intent, intent_revision) = prepared_intent(&f);
    let workspace_request = WorkspaceRequest {
        task_id: f.task_id,
        repository_id: intent.repository_id,
        base_commit: intent.base_commit.clone(),
    };
    task_engine(f.db.clone())
        .assign_task(
            &f.supervisor,
            AssignTask {
                task_id: f.task_id,
                assignee: f.coder_id,
            },
            IdempotencyKey("reassign-before-git".into()),
        )
        .unwrap();
    let error = f
        .manager
        .reconcile_create(
            &f.coder,
            &mut intent,
            intent_revision,
            &assignment,
            assignment_revision,
            &workspace_request,
            &IdempotencyKey("create-after-reassignment".into()),
        )
        .unwrap_err();
    assert!(matches!(error, CoreError::Recoverable(_)));
    assert!(!PathBuf::from(&intent.worktree_path).exists());
}

#[test]
fn release_waits_for_git_intent_and_can_retry_after_reconciliation() {
    let f = fixture(1);
    let (run, assignment, assignment_revision, mut intent, intent_revision) = prepared_intent(&f);
    let (mut mutating, mutating_revision) = start_git_mutation(
        &f,
        &run,
        &assignment,
        assignment_revision,
        &mut intent,
        intent_revision,
    );
    assert!(matches!(
        task_engine(f.db.clone()).release_task(
            &f.supervisor,
            ReleaseTask {
                task_id: f.task_id,
                reason: "must wait".into(),
            },
            IdempotencyKey("release-during-git".into()),
        ),
        Err(CoreError::Recoverable(_))
    ));
    let workspace_request = WorkspaceRequest {
        task_id: f.task_id,
        repository_id: mutating.repository_id,
        base_commit: mutating.base_commit.clone(),
    };
    f.manager
        .reconcile_create(
            &f.coder,
            &mut mutating,
            mutating_revision,
            &assignment,
            assignment_revision + 1,
            &workspace_request,
            &IdempotencyKey("finish-git-after-interruption".into()),
        )
        .unwrap();
    task_engine(f.db.clone())
        .release_task(
            &f.supervisor,
            ReleaseTask {
                task_id: f.task_id,
                reason: "release after Git recorded".into(),
            },
            IdempotencyKey("release-after-git".into()),
        )
        .unwrap();
    assert_eq!(
        f.manager.load_intent(&mutating).unwrap().0.checkpoint,
        GitCheckpoint::Recorded
    );
}

#[test]
fn reassignment_waits_for_git_intent_and_can_retry_after_reconciliation() {
    let f = fixture(1);
    let (run, assignment, assignment_revision, mut intent, intent_revision) = prepared_intent(&f);
    let (mut mutating, mutating_revision) = start_git_mutation(
        &f,
        &run,
        &assignment,
        assignment_revision,
        &mut intent,
        intent_revision,
    );
    assert!(matches!(
        task_engine(f.db.clone()).assign_task(
            &f.supervisor,
            AssignTask {
                task_id: f.task_id,
                assignee: f.coder_id,
            },
            IdempotencyKey("reassign-during-git".into()),
        ),
        Err(CoreError::Recoverable(_))
    ));
    let workspace_request = WorkspaceRequest {
        task_id: f.task_id,
        repository_id: mutating.repository_id,
        base_commit: mutating.base_commit.clone(),
    };
    f.manager
        .reconcile_create(
            &f.coder,
            &mut mutating,
            mutating_revision,
            &assignment,
            assignment_revision + 1,
            &workspace_request,
            &IdempotencyKey("finish-git-before-reassignment".into()),
        )
        .unwrap();
    task_engine(f.db.clone())
        .assign_task(
            &f.supervisor,
            AssignTask {
                task_id: f.task_id,
                assignee: f.coder_id,
            },
            IdempotencyKey("reassign-after-git".into()),
        )
        .unwrap();
}

fn insert<T: serde::Serialize + DeserializeOwned>(
    db: &Database,
    ctx: &SessionContext,
    operation: &str,
    kind: EntityKind,
    id: String,
    scope: EntityScope,
    value: &T,
) {
    let event = NewEvent::from_context(
        ctx,
        ctx.actor_id().0.to_string(),
        EventScope {
            project_id: None,
            run_id: None,
            task_id: None,
        },
        format!("test.{operation}"),
        json!({}),
    );
    db.commit_mutation(MutationInput {
        context: ctx,
        operation,
        idempotency_key: &IdempotencyKey(operation.into()),
        request: value,
        preconditions: vec![],
        result: value,
        changes: vec![StateChange::insert(kind, id, scope, value).unwrap()],
        event,
    })
    .unwrap();
}
fn git<I, S>(repo: &std::path::Path, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn output<I, S>(repo: &std::path::Path, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().into()
}

#[test]
fn multiple_repositories_are_idempotent_capture_commits_and_refuse_dirty_cleanup() {
    let f = fixture(2);
    let mut workspaces = Vec::new();
    for (index, (repository, base_commit)) in f.repos.iter().enumerate() {
        let request = WorkspaceRequest {
            task_id: f.task_id,
            repository_id: repository.id,
            base_commit: base_commit.clone(),
        };
        let key = IdempotencyKey(format!("workspace-{index}"));
        let created = f
            .manager
            .request_workspace(&f.coder, request.clone(), key.clone())
            .unwrap();
        assert!(!created.replayed);
        let repeated = f
            .manager
            .request_workspace(&f.coder, request.clone(), key)
            .unwrap();
        assert!(repeated.replayed);
        assert_eq!(created.value.id, repeated.value.id);
        let new_key = f
            .manager
            .request_workspace(
                &f.coder,
                request,
                IdempotencyKey(format!("workspace-reuse-{index}")),
            )
            .unwrap();
        assert_eq!(created.value.id, new_key.value.id);
        workspaces.push(created.value);
    }
    assert_ne!(workspaces[0].path, workspaces[1].path);
    assert_eq!(workspaces[0].assignment_id, f.assignment_id);
    let workspace = &workspaces[0];
    let worktree = PathBuf::from(&workspace.path);
    fs::write(worktree.join("change.txt"), "captured change\n").unwrap();
    git(&worktree, ["add", "change.txt"]);
    git(&worktree, ["commit", "-m", "worktree change"]);
    let inspection = f
        .manager
        .inspect_workspace(
            &f.coder,
            InspectWorkspace {
                workspace_id: workspace.id,
            },
        )
        .unwrap();
    assert!(inspection.clean);
    let capture = CaptureWorkspaceCommits {
        workspace_id: workspace.id,
        expected_head: inspection.head.commit_id.clone(),
    };
    let captured = f
        .manager
        .capture_commit_refs(&f.coder, capture.clone(), IdempotencyKey("capture".into()))
        .unwrap();
    assert_eq!(captured.value.len(), 1);
    assert_eq!(captured.value[0].commit_id, inspection.head.commit_id);
    assert_eq!(
        f.manager.authors(&captured.value).unwrap(),
        [f.coder_id].into_iter().collect()
    );
    assert!(f
        .manager
        .authors(&[CommitRef {
            repository_id: workspace.repository_id,
            commit_id: "uncaptured".into()
        }])
        .is_err());
    assert!(
        f.manager
            .capture_commit_refs(&f.coder, capture, IdempotencyKey("capture".into()))
            .unwrap()
            .replayed
    );
    assert!(f
        .db
        .load::<WorkspaceCommit>(
            EntityKind::WorkspaceCommit,
            &format!("{}:{}", workspace.id.0, inspection.head.commit_id)
        )
        .unwrap()
        .is_some());
    fs::write(worktree.join("dirty.txt"), "uncommitted").unwrap();
    fs::write(worktree.join("ignored.txt"), "ignored but valuable").unwrap();
    let dirty = f
        .manager
        .inspect_workspace(
            &f.coder,
            InspectWorkspace {
                workspace_id: workspace.id,
            },
        )
        .unwrap();
    assert!(!dirty.clean);
    assert!(dirty.changed_paths.contains(&"dirty.txt".into()));
    assert!(dirty.changed_paths.contains(&"ignored.txt".into()));
    assert!(matches!(
        f.manager.cleanup_workspace(
            &f.supervisor,
            CleanupWorkspace {
                workspace_id: workspace.id
            },
            IdempotencyKey("dirty-cleanup".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    assert!(worktree.exists());
    fs::remove_file(worktree.join("dirty.txt")).unwrap();
    fs::remove_file(worktree.join("ignored.txt")).unwrap();
    f.manager
        .cleanup_workspace(
            &f.supervisor,
            CleanupWorkspace {
                workspace_id: workspace.id,
            },
            IdempotencyKey("clean-cleanup".into()),
        )
        .unwrap();
    assert!(!worktree.exists());
    let repo_path = PathBuf::from(&f.repos[0].0.canonical_path);
    assert!(!Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{}", workspace.branch)
        ])
        .status()
        .unwrap()
        .success());
}

#[test]
fn conflicting_branch_and_path_are_preserved_without_overwrite() {
    let f = fixture(2);
    let (repo1, base1) = &f.repos[0];
    let expected_branch = format!("tesselate/{}/{}", f.assignment_id, repo1.id.0);
    git(
        PathBuf::from(&repo1.canonical_path).as_path(),
        ["branch", &expected_branch, base1],
    );
    let result = f.manager.request_workspace(
        &f.coder,
        WorkspaceRequest {
            task_id: f.task_id,
            repository_id: repo1.id,
            base_commit: base1.clone(),
        },
        IdempotencyKey("branch-conflict".into()),
    );
    assert!(matches!(result, Err(CoreError::Recoverable(_))));
    assert_eq!(
        output(
            PathBuf::from(&repo1.canonical_path).as_path(),
            ["rev-parse", &expected_branch]
        ),
        *base1
    );
    let (repo2, base2) = &f.repos[1];
    let conflict_path = f.manager.workspace_path(&f.assignment_id, repo2.id);
    fs::create_dir_all(&conflict_path).unwrap();
    fs::write(conflict_path.join("keep.txt"), "unrelated").unwrap();
    let result = f.manager.request_workspace(
        &f.coder,
        WorkspaceRequest {
            task_id: f.task_id,
            repository_id: repo2.id,
            base_commit: base2.clone(),
        },
        IdempotencyKey("path-conflict".into()),
    );
    assert!(matches!(result, Err(CoreError::Recoverable(_))));
    assert_eq!(
        fs::read_to_string(conflict_path.join("keep.txt")).unwrap(),
        "unrelated"
    );
}

#[test]
fn interrupted_git_add_is_reconciled_from_persisted_intent() {
    let f = fixture(1);
    let (repository, base) = &f.repos[0];
    let repo = PathBuf::from(&repository.canonical_path);
    let path = f.manager.workspace_path(&f.assignment_id, repository.id);
    let branch = format!("tesselate/{}/{}", f.assignment_id, repository.id.0);
    let intent = GitOperationIntent {
        id: GitOperationId::new(),
        assignment_key: f.assignment_id.clone(),
        repository_id: repository.id,
        canonical_repository_path: repository.canonical_path.clone(),
        base_commit: base.clone(),
        branch: branch.clone(),
        worktree_path: path.to_string_lossy().into_owned(),
        owner: f.coder_id,
        checkpoint: GitCheckpoint::MutationStarted,
        observed: None,
        result: None,
        recovery_reason: None,
    };
    let run =
        f.db.load::<Run>(EntityKind::Run, &f.run_id.0.to_string())
            .unwrap()
            .unwrap()
            .0;
    let event = f
        .manager
        .event(&f.supervisor, &run, f.task_id, "test.intent", json!({}));
    f.db.commit_mutation(MutationInput {
        context: &f.supervisor,
        operation: "test.intent",
        idempotency_key: &IdempotencyKey("interrupt-intent".into()),
        request: &intent,
        preconditions: vec![],
        result: &intent,
        changes: vec![StateChange::insert(
            EntityKind::GitOperation,
            intent.id.0.to_string(),
            intent_scope(f.project_id, &intent),
            &intent,
        )
        .unwrap()],
        event,
    })
    .unwrap();
    git(
        &repo,
        [
            "worktree",
            "add",
            "-b",
            &branch,
            "--",
            path.to_str().unwrap(),
            base.as_str(),
        ],
    );
    let reopened_db = Database::open(&f.db_path).unwrap();
    let reopened_manager = GitWorkspaceManager::new(reopened_db.clone(), &f.manager.root).unwrap();
    let recovered = reopened_manager
        .request_workspace(
            &f.coder,
            WorkspaceRequest {
                task_id: f.task_id,
                repository_id: repository.id,
                base_commit: base.clone(),
            },
            IdempotencyKey("reconcile".into()),
        )
        .unwrap();
    assert_eq!(recovered.value.assignment_id, f.assignment_id);
    assert_eq!(recovered.value.base_commit, *base);
    assert_eq!(
        f.db.load::<GitOperationIntent>(EntityKind::GitOperation, &intent.id.0.to_string())
            .unwrap()
            .unwrap()
            .0
            .checkpoint,
        GitCheckpoint::Recorded
    );
    let rows = output(&repo, ["worktree", "list", "--porcelain"]);
    assert!(rows.contains(path.to_str().unwrap()));
    assert_eq!(rows.matches("worktree ").count(), 2);
}

#[test]
fn superseded_assignment_cannot_create_a_workspace() {
    let f = fixture(1);
    let (repository, base) = &f.repos[0];
    let (mut assignment, revision) =
        f.db.load::<AssignmentRecord>(EntityKind::Assignment, &f.assignment_id)
            .unwrap()
            .unwrap();
    assignment.status = AssignmentStatus::Superseded;
    let event = f.manager.event(
        &f.supervisor,
        &f.db
            .load::<Run>(EntityKind::Run, &f.run_id.0.to_string())
            .unwrap()
            .unwrap()
            .0,
        f.task_id,
        "test.assignment_superseded",
        json!({}),
    );
    f.db.commit_mutation(MutationInput {
        context: &f.supervisor,
        operation: "test.assignment_superseded",
        idempotency_key: &IdempotencyKey("assignment-superseded".into()),
        request: &assignment,
        preconditions: vec![],
        result: &assignment,
        changes: vec![StateChange::update(
            EntityKind::Assignment,
            f.assignment_id.clone(),
            EntityScope {
                project_id: Some(f.project_id.0.to_string()),
                run_id: Some(f.run_id.0.to_string()),
                task_id: Some(f.task_id.0.to_string()),
                owner_id: Some(f.coder_id.0.to_string()),
            },
            revision,
            &assignment,
        )
        .unwrap()],
        event,
    })
    .unwrap();
    assert!(matches!(
        f.manager.request_workspace(
            &f.coder,
            WorkspaceRequest {
                task_id: f.task_id,
                repository_id: repository.id,
                base_commit: base.clone()
            },
            IdempotencyKey("stale-assignment".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    assert!(!f
        .manager
        .workspace_path(&f.assignment_id, repository.id)
        .exists());
}

#[test]
fn cleanup_removal_before_checkpoint_reconciles_without_recreating_workspace() {
    let f = fixture(1);
    let (repository, base) = &f.repos[0];
    let created = f
        .manager
        .request_workspace(
            &f.coder,
            WorkspaceRequest {
                task_id: f.task_id,
                repository_id: repository.id,
                base_commit: base.clone(),
            },
            IdempotencyKey("create-before-cleanup".into()),
        )
        .unwrap();
    let (mut intent, revision) = f.manager.intent_for_workspace(&created.value).unwrap();
    intent.checkpoint = GitCheckpoint::CleanupStarted;
    let run =
        f.db.load::<Run>(EntityKind::Run, &f.run_id.0.to_string())
            .unwrap()
            .unwrap()
            .0;
    f.manager
        .save_intent_step(
            &f.supervisor,
            &run,
            f.task_id,
            intent.clone(),
            revision,
            "workspace.cleanup_started",
            json!({}),
            vec![],
        )
        .unwrap();
    let repo_path = PathBuf::from(&repository.canonical_path);
    git(
        &repo_path,
        ["worktree", "remove", "--", &created.value.path],
    );
    git(&repo_path, ["branch", "-D", "--", &created.value.branch]);
    let reopened_db = Database::open(&f.db_path).unwrap();
    let reopened_manager = GitWorkspaceManager::new(reopened_db.clone(), &f.manager.root).unwrap();
    let cleaned = reopened_manager
        .cleanup_workspace(
            &f.supervisor,
            CleanupWorkspace {
                workspace_id: created.value.id,
            },
            IdempotencyKey("reconcile-cleanup".into()),
        )
        .unwrap();
    assert_eq!(cleaned.value.id, created.value.id);
    let (persisted, _) =
        f.db.load::<GitOperationIntent>(EntityKind::GitOperation, &intent.id.0.to_string())
            .unwrap()
            .unwrap();
    assert_eq!(persisted.checkpoint, GitCheckpoint::Cleaned);
    assert!(matches!(
        f.manager.request_workspace(
            &f.coder,
            WorkspaceRequest {
                task_id: f.task_id,
                repository_id: repository.id,
                base_commit: base.clone()
            },
            IdempotencyKey("no-resurrection".into())
        ),
        Err(CoreError::Recoverable(_))
    ));
    assert!(!PathBuf::from(&created.value.path).exists());
}

#[test]
fn concurrent_requests_share_one_assignment_repository_workspace() {
    let f = fixture(1);
    let (repository, base) = &f.repos[0];
    let request = WorkspaceRequest {
        task_id: f.task_id,
        repository_id: repository.id,
        base_commit: base.clone(),
    };
    let manager_a = f.manager.clone();
    let context_a = f.coder.clone();
    let request_a = request.clone();
    let handle_a = std::thread::spawn(move || {
        manager_a.request_workspace(&context_a, request_a, IdempotencyKey("parallel-a".into()))
    });
    let manager_b = f.manager.clone();
    let context_b = f.coder.clone();
    let request_b = request.clone();
    let handle_b = std::thread::spawn(move || {
        manager_b.request_workspace(&context_b, request_b, IdempotencyKey("parallel-b".into()))
    });
    let first = handle_a.join().unwrap();
    let second = handle_b.join().unwrap();
    let first = first.unwrap_or_else(|_| {
        f.manager
            .request_workspace(
                &f.coder,
                request.clone(),
                IdempotencyKey("parallel-a".into()),
            )
            .unwrap()
    });
    let second = second.unwrap_or_else(|_| {
        f.manager
            .request_workspace(&f.coder, request, IdempotencyKey("parallel-b".into()))
            .unwrap()
    });
    assert_eq!(first.value.id, second.value.id);
    let intents =
        f.db.list_entities::<GitOperationIntent>(
            EntityKind::GitOperation,
            &EntityScope {
                project_id: Some(f.project_id.0.to_string()),
                ..Default::default()
            },
            PageRequest {
                limit: 10,
                after: None,
            },
        )
        .unwrap();
    assert_eq!(intents.items.len(), 1);
}
