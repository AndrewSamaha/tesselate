//! TOML project configuration parsing and local-path validation (F01).

use crate::core::{
    ConfiguredRepository, HumanGatePolicy, ReviewPolicy, ValidatedProjectConfig, WorkflowLimits,
};
use serde::Deserialize;
use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io,
    path::{Component, Path, PathBuf},
    process::Command,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read project configuration {path}: {source}")]
    Read { path: PathBuf, source: io::Error },
    #[error("invalid project TOML: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid project configuration: {0}")]
    Invalid(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProjectConfig {
    name: String,
    artifact_dir: PathBuf,
    repositories: Vec<RawRepository>,
    workflow: WorkflowLimits,
    review: ReviewPolicy,
    human: Option<HumanGatePolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRepository {
    id: String,
    path: PathBuf,
}

/// Parses and validates configuration using `base_dir` to resolve relative paths.
pub fn parse_project_config(
    source: &str,
    base_dir: impl AsRef<Path>,
) -> Result<ValidatedProjectConfig, ConfigError> {
    let raw: RawProjectConfig = toml::from_str(source)?;
    validate(raw, base_dir.as_ref())
}

/// Loads a project configuration file; relative paths are based on its containing directory.
pub fn load_project_config(path: impl AsRef<Path>) -> Result<ValidatedProjectConfig, ConfigError> {
    let path = path.as_ref();
    let source = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_owned(),
        source,
    })?;
    let base = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    parse_project_config(&source, base)
}

fn validate(raw: RawProjectConfig, base_dir: &Path) -> Result<ValidatedProjectConfig, ConfigError> {
    let name = raw.name.trim();
    if name.is_empty() {
        return Err(ConfigError::Invalid(
            "project name must not be empty".into(),
        ));
    }
    validate_limits(&raw.workflow)?;
    if raw.review.min_independent_reviewers < 2 {
        return Err(ConfigError::Invalid(
            "review.min_independent_reviewers must be at least 2".into(),
        ));
    }
    if raw.repositories.is_empty() {
        return Err(ConfigError::Invalid(
            "at least one [[repositories]] entry is required".into(),
        ));
    }

    let base = fs::canonicalize(base_dir).map_err(|error| {
        ConfigError::Invalid(format!(
            "configuration base directory {} cannot be resolved: {error}",
            base_dir.display()
        ))
    })?;
    let mut ids = HashSet::new();
    let mut paths = HashSet::new();
    let mut repositories = Vec::with_capacity(raw.repositories.len());
    for repository in raw.repositories {
        if repository.path.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "repository path for {:?} must not be empty",
                repository.id
            )));
        }
        let id = repository.id.trim();
        if !valid_repository_id(id) {
            return Err(ConfigError::Invalid(format!(
                "repository ID {:?} must start with an ASCII letter or digit and contain only letters, digits, '.', '_' or '-'",
                repository.id
            )));
        }
        if !ids.insert(id.to_owned()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate repository ID {id:?}"
            )));
        }
        let canonical_path = canonical_git_repository(&base, &repository.path, id)?;
        if !paths.insert(canonical_path.clone()) {
            return Err(ConfigError::Invalid(format!(
                "repository {id:?} resolves to a path already registered by another repository: {}",
                canonical_path.display()
            )));
        }
        repositories.push(ConfiguredRepository {
            id: id.to_owned(),
            canonical_path: canonical_path.to_string_lossy().into_owned(),
        });
    }

    if raw.artifact_dir.as_os_str().is_empty() {
        return Err(ConfigError::Invalid(
            "artifact_dir must not be empty".into(),
        ));
    }
    let artifact_dir = resolve_uncreated_path(&base, &raw.artifact_dir)?;
    if artifact_dir.exists() && !artifact_dir.is_dir() {
        return Err(ConfigError::Invalid(format!(
            "artifact_dir {} exists but is not a directory",
            artifact_dir.display()
        )));
    }
    ensure_writable_directory(&artifact_dir)?;

    Ok(ValidatedProjectConfig {
        name: name.to_owned(),
        repositories,
        artifact_dir: artifact_dir.to_string_lossy().into_owned(),
        workflow_limits: raw.workflow,
        review_policy: raw.review,
        human_gates: raw.human,
    })
}

fn validate_limits(limits: &WorkflowLimits) -> Result<(), ConfigError> {
    if limits.max_duration_hours == 0
        || limits.max_concurrent_assignments == 0
        || limits.max_task_retries == 0
        || limits.max_review_rounds == 0
    {
        return Err(ConfigError::Invalid(
            "workflow limits must all be greater than zero".into(),
        ));
    }
    Ok(())
}

fn valid_repository_id(id: &str) -> bool {
    let mut chars = id.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphanumeric())
        && chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

fn canonical_git_repository(
    base: &Path,
    configured: &Path,
    id: &str,
) -> Result<PathBuf, ConfigError> {
    let path = if configured.is_absolute() {
        configured.to_owned()
    } else {
        base.join(configured)
    };
    let canonical = fs::canonicalize(&path).map_err(|error| {
        ConfigError::Invalid(format!(
            "repository {id:?} path {} does not exist or cannot be resolved: {error}",
            path.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(ConfigError::Invalid(format!(
            "repository {id:?} path {} is not a directory",
            canonical.display()
        )));
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(&canonical)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .map_err(|error| {
            ConfigError::Invalid(format!(
                "cannot run git to validate repository {id:?} at {}: {error}",
                canonical.display()
            ))
        })?;
    if !output.status.success() {
        return Err(ConfigError::Invalid(format!(
            "repository {id:?} path {} is not an existing Git working tree",
            canonical.display()
        )));
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let root = fs::canonicalize(&root).map_err(|error| {
        ConfigError::Invalid(format!(
            "Git reported an invalid root for repository {id:?}: {error}"
        ))
    })?;
    if root != canonical {
        return Err(ConfigError::Invalid(format!(
            "repository {id:?} path {} is inside a Git worktree; configure its root {} instead",
            canonical.display(),
            root.display()
        )));
    }
    Ok(root)
}

fn resolve_uncreated_path(base: &Path, configured: &Path) -> Result<PathBuf, ConfigError> {
    let path = if configured.is_absolute() {
        configured.to_owned()
    } else {
        base.join(configured)
    };
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    let mut ancestor = normalized.as_path();
    let mut suffix = Vec::new();
    while matches!(
        fs::symlink_metadata(ancestor),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    ) {
        let name = ancestor.file_name().ok_or_else(|| {
            ConfigError::Invalid(format!(
                "artifact_dir {} has no existing parent directory",
                normalized.display()
            ))
        })?;
        suffix.push(name.to_os_string());
        ancestor = ancestor.parent().ok_or_else(|| {
            ConfigError::Invalid(format!(
                "artifact_dir {} has no existing parent directory",
                normalized.display()
            ))
        })?;
    }
    if !ancestor.is_dir() {
        return Err(ConfigError::Invalid(format!(
            "artifact_dir parent {} is not a directory",
            ancestor.display()
        )));
    }
    let mut resolved = fs::canonicalize(ancestor).map_err(|error| {
        ConfigError::Invalid(format!(
            "artifact_dir parent {} cannot be resolved: {error}",
            ancestor.display()
        ))
    })?;
    for component in suffix.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn ensure_writable_directory(destination: &Path) -> Result<(), ConfigError> {
    let mut ancestor = destination;
    while matches!(
        fs::symlink_metadata(ancestor),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    ) {
        ancestor = ancestor.parent().ok_or_else(|| {
            ConfigError::Invalid(format!(
                "artifact_dir {} has no writable parent directory",
                destination.display()
            ))
        })?;
    }
    let probe = ancestor.join(format!(".tesselate-write-check-{}", Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|error| {
            ConfigError::Invalid(format!(
                "artifact_dir {} is not writable (cannot create a temporary file in {}): {error}",
                destination.display(),
                ancestor.display()
            ))
        })?;
    drop(file);
    fs::remove_file(&probe).map_err(|error| {
        ConfigError::Invalid(format!(
            "cannot remove artifact directory write-check file {}: {error}",
            probe.display()
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests;
