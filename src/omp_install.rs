//! User-scoped OMP installation and default per-repository registration.
use super::{credential_response, supervisor_context, CliError};
use crate::{
    config::parse_project_config,
    coordination::{application::Application, Coordination},
    core::{CoreError, EventCursor, IdempotencyKey, PageRequest, Role, SessionContext},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
};
use uuid::Uuid;

const EXTENSION: &str = include_str!("../omp/tesselate.ts");
const SKILL: &str = include_str!("../omp/skills/tesselate-feature/SKILL.md");
const MANIFEST: &str = "omp-install.json";

#[derive(Serialize, Deserialize)]
struct BridgeConfig {
    binary: PathBuf,
    state_dir: PathBuf,
    workspace_root: PathBuf,
    supervisor_credential_file: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct OwnedFile {
    path: PathBuf,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    agent_dir: PathBuf,
    credential_file: PathBuf,
    files: Vec<OwnedFile>,
}

fn home() -> Result<PathBuf, CliError> {
    let home = env::var_os("HOME").ok_or_else(|| CoreError::Invalid("HOME is required for OMP integration".into()))?;
    Ok(PathBuf::from(home))
}

pub(super) fn workspace_root() -> Result<PathBuf, CliError> {
    let path = env::var_os("TESSELATE_WORKSPACE_ROOT").map(PathBuf::from)
        .unwrap_or(home()?.join("Development"));
    if !path.is_absolute() {
        return Err(CoreError::Invalid("TESSELATE_WORKSPACE_ROOT must be absolute".into()).into());
    }
    Ok(path)
}

fn agent_dir() -> Result<PathBuf, CliError> {
    let first = env::var_os("OMP_PROFILE");
    let second = env::var_os("PI_PROFILE");
    if first.is_some() && second.is_some() && first != second {
        return Err(CoreError::Invalid("OMP_PROFILE and PI_PROFILE disagree".into()).into());
    }
    let base = home()?.join(".omp");
    if let Some(profile) = first.or(second) {
        let profile = profile.to_string_lossy();
        if profile.is_empty() || !profile.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_') {
            return Err(CoreError::Invalid("OMP profile name must contain only letters, digits, '-' or '_'".into()).into());
        }
        Ok(base.join("profiles").join(profile.as_ref()).join("agent"))
    } else {
        Ok(base.join("agent"))
    }
}

fn targets(agent: &Path) -> [PathBuf; 3] {
    [agent.join("extensions/tesselate.ts"), agent.join("skills/tesselate-feature/SKILL.md"), agent.join("extensions/tesselate.config.json")]
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn existing_file(path: &Path) -> Result<Option<Vec<u8>>, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(Some(fs::read(path)?)),
        Ok(_) => Err(CoreError::Invalid(format!("refusing non-regular OMP integration file {}", path.display())).into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_manifest(state: &Path) -> Result<Option<Manifest>, CliError> {
    let path = state.join(MANIFEST);
    existing_file(&path)?.map(|data| serde_json::from_slice(&data).map_err(Into::into)).transpose()
}

fn private_file(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn atomic_private_file(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    let temporary = path.with_file_name(format!(".tesselate-{}", Uuid::new_v4()));
    private_file(&temporary, bytes)?;
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error.into());
    }
    Ok(())
}

fn create_directory(path: &Path) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        if parent != path { create_directory(parent)?; }
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(CoreError::Invalid(format!("refusing non-directory or symlink {}", path.display())).into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            #[cfg(unix)] {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_ownership(manifest: Option<&Manifest>, agent: &Path, paths: &[PathBuf; 3]) -> Result<(), CliError> {
    if let Some(manifest) = manifest {
        if manifest.agent_dir != agent || manifest.files.len() != paths.len()
            || manifest.files.iter().zip(paths).any(|(owned, path)| owned.path != *path) {
            return Err(CoreError::Invalid("OMP installation manifest belongs to a different profile or has unexpected paths".into()).into());
        }
    }
    for path in paths {
        if let Some(data) = existing_file(path)? {
            let owned = manifest.and_then(|manifest| manifest.files.iter().find(|owned| owned.path == *path));
            if owned.is_none_or(|owned| owned.sha256 != digest(&data)
                && owned.previous_sha256.as_deref() != Some(digest(&data).as_str())) {
                return Err(CoreError::Invalid(format!("refusing to overwrite unowned or modified OMP file {}", path.display())).into());
            }
        }
    }
    Ok(())
}

pub(super) fn supervisor_credential_file(state: &Path, explicit: Option<&Path>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit { return credential_path(path); }
    let agent = agent_dir()?;
    let path = targets(&agent)[2].clone();
    let manifest = read_manifest(state)?.ok_or_else(|| CoreError::Invalid("OMP is not installed; run tesselate install omp or specify --credential-file".into()))?;
    verify_ownership(Some(&manifest), &agent, &targets(&agent))?;
    let config: BridgeConfig = serde_json::from_slice(&fs::read(path)?)?;
    if config.state_dir != state || config.supervisor_credential_file != manifest.credential_file {
        return Err(CoreError::Invalid("OMP installation configuration does not match this state directory".into()).into());
    }
    credential_path(&manifest.credential_file)
}

fn configured_workspace_root(state: &Path) -> Result<PathBuf, CliError> {
    let Some(manifest) = read_manifest(state)? else { return workspace_root(); };
    let agent = agent_dir()?;
    let paths = targets(&agent);
    verify_ownership(Some(&manifest), &agent, &paths)?;
    let config: BridgeConfig = serde_json::from_slice(&fs::read(&paths[2])?)?;
    if config.state_dir != state {
        return Err(CoreError::Invalid("OMP installation belongs to another state directory".into()).into());
    }
    Ok(config.workspace_root)
}

fn credential_path(path: &Path) -> Result<PathBuf, CliError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(CoreError::Invalid("supervisor credential must be a regular file, not a symlink".into()).into());
    }
    #[cfg(unix)] {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.permissions().mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(CoreError::Invalid("supervisor credential must be owned by the current user and mode 0600".into()).into());
        }
    }
    Ok(fs::canonicalize(path)?)
}

fn local_supervisor_context(app: &Application, path: &Path) -> Result<SessionContext, CliError> {
    let path = credential_path(path)?;
    match supervisor_context(app, Some(&path)) {
        Ok(ctx) => Ok(ctx),
        Err(CliError::Core(CoreError::Unauthenticated)) => {
            let secret = fs::read_to_string(path)?;
            Ok(app.recover_local_supervisor(secret.trim())?)
        }
        Err(error) => Err(error),
    }
}

fn select_credential(app: &Application, state: &Path, explicit: Option<&Path>, prior: Option<&Manifest>) -> Result<PathBuf, CliError> {
    if let Some(path) = explicit.or_else(|| prior.map(|manifest| manifest.credential_file.as_path())) {
        let path = credential_path(path)?;
        local_supervisor_context(app, &path)?;
        return Ok(path);
    }
    let credentials = state.join("credentials");
    let mut active = Vec::new();
    let mut expired_candidates = Vec::new();
    if credentials.is_dir() {
        for entry in fs::read_dir(&credentials)? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "token") {
                let Ok(path) = credential_path(&path) else { continue; };
                match supervisor_context(app, Some(&path)) {
                    Ok(ctx) if ctx.role() == Role::Supervisor => active.push(path),
                    Err(CliError::Core(CoreError::Unauthenticated)) => expired_candidates.push(path),
                    _ => {}
                }
            }
        }
    }
    match active.len() {
        1 => return Ok(active.remove(0)),
        2.. => return Err(CoreError::Invalid("multiple supervisor credentials found; select one with --credential-file".into()).into()),
        _ => {}
    }
    if expired_candidates.len() == 1 {
        let path = expired_candidates.remove(0);
        if local_supervisor_context(app, &path).is_ok() { return Ok(path); }
    }
    if expired_candidates.len() > 1 {
        return Err(CoreError::Invalid("multiple inactive credentials found; select a supervisor with --credential-file".into()).into());
    }
    let response = credential_response(state, app.bootstrap_supervisor(
        &app.prepare_bootstrap()?, "omp-supervisor", IdempotencyKey(format!("omp-bootstrap:{}", Uuid::new_v4())),
    )?)?;
    let file: PathBuf = serde_json::from_value(response["credential_file"].clone())?;
    Ok(file)
}

pub(super) fn install(app: &Application, state: &Path, credential: Option<&Path>) -> Result<Value, CliError> {
    let agent = agent_dir()?;
    let workspace = fs::canonicalize(workspace_root()?)?;
    let binary = fs::canonicalize(env::current_exe()?)?;
    install_with_agent(app, state, credential, &agent, &workspace, &binary)
}

fn install_with_agent(app: &Application, state: &Path, credential: Option<&Path>, agent: &Path, workspace: &Path, binary: &Path) -> Result<Value, CliError> {
    let paths = targets(agent);
    let prior = read_manifest(state)?;
    verify_ownership(prior.as_ref(), &agent, &paths)?;
    let workspace = workspace.to_owned();
    let selected = select_credential(app, state, credential, prior.as_ref())?;
    let config = BridgeConfig {
        binary: binary.to_owned(),
        state_dir: state.to_owned(),
        workspace_root: workspace,
        supervisor_credential_file: selected.clone(),
    };
    let contents = [EXTENSION.as_bytes().to_vec(), SKILL.as_bytes().to_vec(), serde_json::to_vec_pretty(&config)?];
    let manifest = Manifest {
        agent_dir: agent.to_owned(), credential_file: selected,
        files: paths.iter().zip(&contents).enumerate().map(|(index, (path, bytes))| {
            let sha256 = digest(bytes);
            let previous_sha256 = prior.as_ref().and_then(|record| record.files.get(index)).and_then(|file| {
                if file.sha256 == sha256 { file.previous_sha256.clone() }
                else { Some(file.sha256.clone()) }
            });
            OwnedFile { path: path.clone(), sha256, previous_sha256 }
        }).collect(),
    };
    // Record both accepted generations before changing files: a crash can resume either state.
    if prior.is_none() {
        private_file(&state.join(MANIFEST), &serde_json::to_vec_pretty(&manifest)?)?;
    } else {
        atomic_private_file(&state.join(MANIFEST), &serde_json::to_vec_pretty(&manifest)?)?;
    }
    for (index, (path, content)) in paths.iter().zip(contents.iter()).enumerate() {
        create_directory(path.parent().expect("OMP asset has parent"))?;
        match existing_file(path)? {
            Some(bytes) if bytes == *content => {}
            Some(bytes) => {
                let owned = &manifest.files[index];
                if owned.previous_sha256.as_deref() != Some(digest(&bytes).as_str()) {
                    return Err(CoreError::Invalid("OMP file changed during installation".into()).into());
                }
                atomic_private_file(path, content)?;
            }
            None => private_file(path, content)?,
        }
    }
    let final_manifest = Manifest {
        files: manifest.files.into_iter().map(|mut file| { file.previous_sha256 = None; file }).collect(),
        ..manifest
    };
    atomic_private_file(&state.join(MANIFEST), &serde_json::to_vec_pretty(&final_manifest)?)?;
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&paths[2], fs::Permissions::from_mode(0o600))?;
    }
    Ok(json!({"installed": true, "extension": paths[0], "skill": paths[1], "config": paths[2], "state_dir": state}))
}

pub(super) fn uninstall(state: &Path) -> Result<Value, CliError> {
    uninstall_with_agent(state, &agent_dir()?)
}

fn uninstall_with_agent(state: &Path, agent: &Path) -> Result<Value, CliError> {
    let Some(manifest) = read_manifest(state)? else { return Ok(json!({"uninstalled": false, "preserved_modified": []})); };
    let paths = targets(agent);
    if manifest.agent_dir != agent || manifest.files.len() != paths.len()
        || manifest.files.iter().zip(&paths).any(|(owned, path)| owned.path != *path) {
        return Err(CoreError::Invalid("OMP installation belongs to another profile or manifest paths changed".into()).into());
    }
    let mut preserved = Vec::new();
    for owned in &manifest.files {
        match existing_file(&owned.path) {
            Ok(Some(data)) if digest(&data) == owned.sha256
                || owned.previous_sha256.as_deref() == Some(digest(&data).as_str()) => { fs::remove_file(&owned.path)?; }
            Ok(None) => {}
            Ok(Some(_)) | Err(_) => preserved.push(owned.path.clone()),
        }
    }
    fs::remove_file(state.join(MANIFEST))?;
    Ok(json!({"uninstalled": true, "preserved_modified": preserved, "state_dir": state}))
}

pub(super) fn ensure_project(app: &Application, state: &Path, credential: Option<&Path>, repo: &Path) -> Result<Value, CliError> {
    let workspace = fs::canonicalize(configured_workspace_root(state)?)?;
    ensure_project_with_workspace(app, state, credential, repo, &workspace)
}

fn ensure_project_with_workspace(app: &Application, state: &Path, credential: Option<&Path>, repo: &Path, workspace: &Path) -> Result<Value, CliError> {
    if !repo.is_absolute() { return Err(CoreError::Invalid("--repo must be an absolute path".into()).into()); }
    let directory = fs::canonicalize(repo)?;
    if !directory.is_dir() { return Err(CoreError::Invalid("--repo must be a directory".into()).into()); }
    let output = Command::new("git").arg("-C").arg(&directory).args(["rev-parse", "--show-toplevel"]).output()?;
    if !output.status.success() { return Err(CoreError::Invalid("--repo is not a Git working tree".into()).into()); }
    let reported = String::from_utf8(output.stdout).map_err(|_| CoreError::Invalid("Git returned a non-UTF-8 root".into()))?;
    let canonical = fs::canonicalize(reported.trim())?;
    if !canonical.starts_with(workspace) || canonical == workspace {
        return Err(CoreError::Invalid(format!("repository must be within workspace root {}", workspace.display())).into());
    }
    let selected = supervisor_credential_file(state, credential)?;
    let ctx = local_supervisor_context(app, &selected)?;
    let path = canonical.to_string_lossy().into_owned();
    let mut next = None;
    loop {
        let page = app.list_projects(&ctx, PageRequest { limit: 500, after: next.map(EventCursor) })?;
        for project in page.items {
            let mut repo_next = None;
            loop {
                let repos = app.list_repositories(&ctx, project.id, PageRequest { limit: 500, after: repo_next.map(EventCursor) })?;
                if let Some(found) = repos.items.iter().find(|entry| entry.canonical_path == path) {
                    return Ok(json!({"project_id": project.id, "repository_id": found.id, "canonical_path": path}));
                }
                repo_next = repos.next_cursor;
                if repo_next.is_none() { break; }
            }
        }
        next = page.next_cursor;
        if next.is_none() { break; }
    }
    let hash = digest(path.as_bytes());
    let name = canonical.file_name().and_then(|name| name.to_str()).unwrap_or("repository");
    let project_dir = state.join("projects").join(&hash);
    create_directory(&project_dir)?;
    let config_path = project_dir.join("project.toml");
    let config = format!(
        "name = {}\nartifact_dir = {}\n[[repositories]]\nid = \"main\"\npath = {}\n[workflow]\nmax_duration_hours = 24\nmax_concurrent_assignments = 2\nmax_task_retries = 2\nmax_review_rounds = 3\n[review]\nmin_independent_reviewers = 2\n",
        toml::Value::String(format!("{name}-{}", &hash[..12])),
        toml::Value::String(project_dir.join("artifacts").to_string_lossy().into_owned()),
        toml::Value::String(path.clone()),
    );
    let validated = parse_project_config(&config, &project_dir)?;
    if let Some(existing) = existing_file(&config_path)? {
        if existing != config.as_bytes() {
            return Err(CoreError::Invalid(format!("existing project configuration {} differs from generated default", config_path.display())).into());
        }
    } else { private_file(&config_path, config.as_bytes())?; }
    let registered = app.register_project(&ctx, validated, &config_path, IdempotencyKey(format!("project.ensure:{hash}")))?;
    let repos = app.list_repositories(&ctx, registered.value.id, PageRequest { limit: 10, after: None })?;
    let found = repos.items.into_iter().find(|item| item.canonical_path == path)
        .ok_or_else(|| CoreError::Internal("registered repository was not found".into()))?;
    Ok(json!({"project_id": registered.value.id, "repository_id": found.id, "canonical_path": path}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cli::prepare_state_dir, persistence::Database};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = env::temp_dir().join(format!("tesselate-omp-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }

        fn app(&self) -> (Application, PathBuf, PathBuf) {
            let state = prepare_state_dir(&self.0.join("state")).unwrap();
            let db = Database::open(state.join("coordinator.sqlite3")).unwrap();
            let app = Application::new(db, state.join("worktrees")).unwrap();
            let response = credential_response(&state, app.bootstrap_supervisor(
                &app.prepare_bootstrap().unwrap(), "test-supervisor", IdempotencyKey("test-bootstrap".into())
            ).unwrap()).unwrap();
            let credential = serde_json::from_value(response["credential_file"].clone()).unwrap();
            (app, state, credential)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }

    #[test]
    fn collisions_and_modified_files_are_not_removed() {
        let fixture = Fixture::new();
        let state = prepare_state_dir(&fixture.0.join("state")).unwrap();
        let agent = fixture.0.join("agent");
        let paths = targets(&agent);
        create_directory(paths[0].parent().unwrap()).unwrap();
        fs::write(&paths[0], "someone else's extension").unwrap();
        assert!(verify_ownership(None, &agent, &paths).is_err());
        assert_eq!(fs::read_to_string(&paths[0]).unwrap(), "someone else's extension");

        let mut files = Vec::new();
        for path in &paths {
            create_directory(path.parent().unwrap()).unwrap();
            fs::write(path, "owned").unwrap();
            files.push(OwnedFile { path: path.clone(), sha256: digest(b"owned"), previous_sha256: None });
        }
        let manifest = Manifest { agent_dir: agent.clone(), credential_file: fixture.0.join("token"), files };
        private_file(&state.join(MANIFEST), &serde_json::to_vec(&manifest).unwrap()).unwrap();
        fs::write(&paths[1], "edited by user").unwrap();
        let result = uninstall_with_agent(&state, &agent).unwrap();
        assert_eq!(result["preserved_modified"], json!([paths[1]]));
        assert!(!paths[0].exists());
        assert_eq!(fs::read_to_string(&paths[1]).unwrap(), "edited by user");
        assert!(!paths[2].exists());
        assert!(!state.join(MANIFEST).exists());
    }

    #[test]
    fn installation_is_idempotent_and_does_not_overwrite_user_edits() {
        let fixture = Fixture::new();
        let (app, state, credential) = fixture.app();
        let agent = fixture.0.join("agent");
        let workspace = fixture.0.join("Development");
        fs::create_dir_all(&workspace).unwrap();
        let binary = env::current_exe().unwrap();
        let first = install_with_agent(&app, &state, Some(&credential), &agent, &workspace, &binary).unwrap();
        let second = install_with_agent(&app, &state, Some(&credential), &agent, &workspace, &binary).unwrap();
        assert_eq!(first, second);
        let manifest = read_manifest(&state).unwrap().unwrap();
        assert_eq!(manifest.files.len(), 3);
        let paths = targets(&agent);
        fs::write(&paths[0], "user extension edit").unwrap();
        assert!(install_with_agent(&app, &state, Some(&credential), &agent, &workspace, &binary).is_err());
        assert_eq!(fs::read_to_string(&paths[0]).unwrap(), "user extension edit");
    }

    #[test]
    fn ensure_registers_repository_once_outside_worktree() {
        let fixture = Fixture::new();
        let (app, state, credential) = fixture.app();
        let workspace = fixture.0.join("Development");
        let repo = workspace.join("example");
        fs::create_dir_all(&repo).unwrap();
        assert!(Command::new("git").arg("init").arg("--quiet").arg(&repo).status().unwrap().success());
        let nested = repo.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let first = ensure_project_with_workspace(&app, &state, Some(&credential), &nested, &workspace).unwrap();
        let second = ensure_project_with_workspace(&app, &state, Some(&credential), &repo, &workspace).unwrap();
        assert_eq!(first, second);
        assert_eq!(first["canonical_path"], json!(repo));
        assert!(!repo.join(".tesselate").exists());
        assert!(state.join("projects").is_dir());
    }
}
