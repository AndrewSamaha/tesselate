use super::*;

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("tesselate-f01-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn git_repo(path: &Path) {
    fs::create_dir_all(path).unwrap();
    let status = Command::new("git")
        .arg("init")
        .arg("--quiet")
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success());
}

fn valid_config(repo_lines: &str) -> String {
    format!(
        r#"
name = "example project"
artifact_dir = ".tesselate/artifacts"
{repo_lines}
[workflow]
max_duration_hours = 24
max_concurrent_assignments = 4
max_task_retries = 2
max_review_rounds = 3
[review]
min_independent_reviewers = 2
"#
    )
}

#[test]
fn accepts_multiple_repositories_and_returns_canonical_paths() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("api"));
    git_repo(&temp.0.join("web"));
    let config = valid_config(
        "[[repositories]]\nid = \"api\"\npath = \"api\"\n\
         [[repositories]]\nid = \"web\"\npath = \"web\"",
    );
    let config_path = temp.0.join("project.toml");
    fs::write(&config_path, config).unwrap();
    let validated = load_project_config(&config_path).unwrap();
    assert_eq!(validated.name, "example project");
    assert_eq!(validated.repositories.len(), 2);
    assert_eq!(validated.repositories[0].id, "api");
    assert_eq!(
        validated.repositories[0].canonical_path,
        fs::canonicalize(temp.0.join("api"))
            .unwrap()
            .to_string_lossy()
    );
    assert_eq!(validated.human_gates, None);
    assert!(Path::new(&validated.artifact_dir).is_absolute());
    assert!(!Path::new(&validated.artifact_dir).exists());
}

#[test]
fn absent_and_explicitly_disabled_human_gate_are_distinct() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    let no_table = valid_config("[[repositories]]\nid = \"repo\"\npath = \"repo\"");
    assert_eq!(
        parse_project_config(&no_table, &temp.0)
            .unwrap()
            .human_gates,
        None
    );
    let explicit = format!("{no_table}\n[human]\ncompletion_approval = false\n");
    assert_eq!(
        parse_project_config(&explicit, &temp.0)
            .unwrap()
            .human_gates,
        Some(HumanGatePolicy {
            completion_approval: false,
            high_severity_deferral_approval: false,
        })
    );

    let gated = format!(
        "{no_table}\n[human]\ncompletion_approval = false\nhigh_severity_deferral_approval = true\n"
    );
    assert_eq!(
        parse_project_config(&gated, &temp.0).unwrap().human_gates,
        Some(HumanGatePolicy {
            completion_approval: false,
            high_severity_deferral_approval: true,
        })
    );
}

#[test]
fn duplicate_repository_ids_and_paths_are_rejected() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    let duplicate_id = valid_config(
        "[[repositories]]\nid = \"repo\"\npath = \"repo\"\n\
         [[repositories]]\nid = \"repo\"\npath = \"repo\"",
    );
    assert!(parse_project_config(&duplicate_id, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("duplicate repository ID"));
    let duplicate_path = valid_config(
        "[[repositories]]\nid = \"api\"\npath = \"repo\"\n\
         [[repositories]]\nid = \"web\"\npath = \"repo\"",
    );
    assert!(parse_project_config(&duplicate_path, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("already registered"));
}

#[test]
fn nonexistent_non_git_and_nested_repository_paths_are_rejected() {
    let temp = TempDir::new();
    fs::create_dir(temp.0.join("plain")).unwrap();
    let missing = valid_config("[[repositories]]\nid = \"repo\"\npath = \"missing\"");
    assert!(parse_project_config(&missing, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
    let non_git = valid_config("[[repositories]]\nid = \"repo\"\npath = \"plain\"");
    assert!(parse_project_config(&non_git, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("not an existing Git working tree"));
    git_repo(&temp.0.join("git"));
    fs::create_dir(temp.0.join("git/subdir")).unwrap();
    let nested = valid_config("[[repositories]]\nid = \"repo\"\npath = \"git/subdir\"");
    assert!(parse_project_config(&nested, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("configure its root"));
}

#[test]
fn zero_workflow_limits_and_too_small_review_policy_are_rejected() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    let base = valid_config("[[repositories]]\nid = \"repo\"\npath = \"repo\"");
    for field in [
        "max_duration_hours",
        "max_concurrent_assignments",
        "max_task_retries",
        "max_review_rounds",
    ] {
        let zero = base.replace(
            &format!(
                "{field} = {}",
                match field {
                    "max_duration_hours" => 24,
                    "max_concurrent_assignments" => 4,
                    "max_task_retries" => 2,
                    _ => 3,
                }
            ),
            &format!("{field} = 0"),
        );
        assert!(parse_project_config(&zero, &temp.0)
            .unwrap_err()
            .to_string()
            .contains("greater than zero"));
    }
    let invalid_review = base.replace(
        "min_independent_reviewers = 2",
        "min_independent_reviewers = 1",
    );
    assert!(parse_project_config(&invalid_review, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("at least 2"));
}

#[test]
fn unknown_safety_policy_keys_are_rejected() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    let base = valid_config("[[repositories]]\nid = \"repo\"\npath = \"repo\"");
    let unknown = format!("unknown_policy = true\n{base}");
    assert!(parse_project_config(&unknown, &temp.0).is_err());
    let nested_unknown = base.replace("[workflow]", "[workflow]\nunsafe_force_cleanup = true");
    assert!(parse_project_config(&nested_unknown, &temp.0).is_err());
    let review_unknown = base.replace("[review]", "[review]\nrequire_human_for_critical = true");
    assert!(parse_project_config(&review_unknown, &temp.0).is_err());
    let human_unknown =
        format!("{base}\n[human]\ncompletion_approval = false\nallow_force_cleanup = true\n");
    assert!(parse_project_config(&human_unknown, &temp.0).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn rejects_artifact_destination_without_a_writable_existing_parent() {
    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    let config = valid_config("[[repositories]]\nid = \"repo\"\npath = \"repo\"").replace(
        "artifact_dir = \".tesselate/artifacts\"",
        "artifact_dir = \"/proc/tesselate-f01-artifacts\"",
    );
    assert!(parse_project_config(&config, &temp.0)
        .unwrap_err()
        .to_string()
        .contains("not writable"));
}

#[cfg(unix)]
#[test]
fn rejects_dangling_artifact_symlinks() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new();
    git_repo(&temp.0.join("repo"));
    symlink(temp.0.join("missing-target"), temp.0.join("dangling")).unwrap();
    let base = valid_config("[[repositories]]\nid = \"repo\"\npath = \"repo\"");

    for artifact_dir in ["dangling", "dangling/artifacts"] {
        let config = base.replace(
            "artifact_dir = \".tesselate/artifacts\"",
            &format!("artifact_dir = \"{artifact_dir}\""),
        );
        assert!(parse_project_config(&config, &temp.0).is_err());
    }
}
