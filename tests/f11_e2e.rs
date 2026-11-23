//! Executable F11 acceptance scenario. Drives the real CLI and stdio MCP adapters.
#![cfg(unix)]
use serde_json::{json, Value};
use std::os::unix::net::UnixStream;
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    time::{Duration, Instant},
};
use tesselate::{
    core::{CommitRef, FindingId, ReviewFinding, Run, RunStatus, Task, Workspace},
    mcp::PROTOCOL_VERSION,
    Project, Repository,
};
use uuid::Uuid;

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("tesselate-f11-{}", Uuid::new_v4()));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Cli {
    state: PathBuf,
}
impl Cli {
    fn run(&self, args: &[&str]) -> Value {
        let out = Command::new(env!("CARGO_BIN_EXE_tesselate"))
            .arg("--state-dir")
            .arg(&self.state)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "tesselate {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.stderr.is_empty(),
            "unexpected CLI diagnostics: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).unwrap()
    }
    fn run_fails(&self, args: &[&str], expected: &str) {
        let out = Command::new(env!("CARGO_BIN_EXE_tesselate"))
            .arg("--state-dir")
            .arg(&self.state)
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains(expected));
    }
    fn authed(&self, credential_file: &Path, args: &[&str]) -> Value {
        let mut full = vec!["--credential-file", credential_file.to_str().unwrap()];
        full.extend_from_slice(args);
        self.run(&full)
    }
}

struct Coordinator(Child);
impl Coordinator {
    fn start(state: &Path) -> Self {
        Self(
            Command::new(env!("CARGO_BIN_EXE_tesselate"))
                .arg("--state-dir")
                .arg(state)
                .arg("serve")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_for_coordinator(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if UnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "coordinator socket did not become ready"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
impl Drop for Coordinator {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Mcp(Child, ChildStdin, BufReader<std::process::ChildStdout>, u64);
impl Mcp {
    fn start(socket: &Path, credential: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_tesselate-mcp"))
            .env("TESSELATE_SOCKET", socket)
            .env("TESSELATE_CREDENTIAL", credential)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut client = Self(child, input, output, 1);
        let initialized = client.request(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"f11-scripted-agent","version":"1"}}}));
        assert_eq!(initialized["result"]["serverInfo"]["name"], "tesselate");
        client.notify(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        client
    }
    fn notify(&mut self, v: Value) {
        writeln!(self.1, "{v}").unwrap();
        self.1.flush().unwrap();
    }
    fn request(&mut self, v: Value) -> Value {
        self.notify(v);
        let mut line = String::new();
        self.2.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    }
    fn call(&mut self, tool: &str, params: Value, key: Option<&str>) -> Value {
        let r = self.call_response(tool, params, key);
        let body = &r["result"]["structuredContent"]["outcome"];
        assert_eq!(body["status"], "success", "{tool}: {body}");
        body["data"].clone()
    }
    fn call_error(&mut self, tool: &str, params: Value, key: Option<&str>) -> String {
        let r = self.call_response(tool, params, key);
        let body = &r["result"]["structuredContent"]["outcome"];
        assert_eq!(body["status"], "error", "{tool}: {body}");
        body["data"]["message"].as_str().unwrap().to_owned()
    }
    fn call_response(&mut self, tool: &str, params: Value, key: Option<&str>) -> Value {
        self.3 += 1;
        let mut args = json!({"schema_version":PROTOCOL_VERSION,"request_id":format!("f11-{}",self.3),"params":params});
        if let Some(k) = key {
            args["idempotency_key"] = json!(k);
        }
        let r = self.request(json!({"jsonrpc":"2.0","id":self.3,"method":"tools/call","params":{"name":tool,"arguments":args}}));
        r
    }
}
impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn make_repo(path: &Path, label: &str) -> String {
    fs::create_dir_all(path).unwrap();
    git(path, &["init", "--quiet"]);
    git(path, &["config", "user.name", label]);
    git(path, &["config", "user.email", "f11@example.invalid"]);
    fs::write(path.join("README.md"), format!("{label} starter\n")).unwrap();
    git(path, &["add", "README.md"]);
    git(path, &["commit", "--quiet", "-m", "fixture base"]);
    git(path, &["rev-parse", "HEAD"])
}
fn value<T: serde::de::DeserializeOwned>(v: Value) -> T {
    serde_json::from_value(v).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f11_real_session_acceptance_scenario() {
    let tmp = TempDir::new();
    let state = tmp.0.join("state");
    let repo_a = tmp.0.join("repo-a");
    let repo_b = tmp.0.join("repo-b");
    let base_a = make_repo(&repo_a, "coder-a");
    let base_b = make_repo(&repo_b, "coder-b");
    let config = tmp.0.join("project.toml");
    fs::write(
        &config,
        format!(
            r#"name="f11-autonomous"
artifact_dir="{}"
[[repositories]]
id="repo-a"
path="{}"
[[repositories]]
id="repo-b"
path="{}"
[workflow]
max_duration_hours=24
max_concurrent_assignments=2
max_task_retries=2
max_review_rounds=3
[review]
min_independent_reviewers=2
"#,
            tmp.0.join("artifacts").display(),
            repo_a.display(),
            repo_b.display()
        ),
    )
    .unwrap();
    let cli = Cli {
        state: state.clone(),
    };
    let boot = cli.run(&["credential", "bootstrap", "--actor-name", "supervisor"]);
    let sup_file = PathBuf::from(boot["credential_file"].as_str().unwrap());
    let sup_token = boot["credential"].as_str().unwrap().to_owned();
    let registered = cli.authed(
        &sup_file,
        &[
            "project",
            "register",
            "--config",
            config.to_str().unwrap(),
            "--idempotency-key",
            "f11-register",
        ],
    );
    let project: Project = value(registered["value"].clone());
    let repos: Vec<Repository> = value(
        cli.authed(
            &sup_file,
            &[
                "inspect",
                "repositories",
                "--project-id",
                &project.id.0.to_string(),
            ],
        )["items"]
            .clone(),
    );
    assert_eq!(repos.len(), 2);
    let repo_a_id = repos.iter().find(|r| r.name == "repo-a").unwrap().id;
    let repo_b_id = repos.iter().find(|r| r.name == "repo-b").unwrap().id;

    let mut credentials = Vec::new();
    for (name, role) in [
        ("coder-a", "coder"),
        ("coder-b", "coder"),
        ("reviewer-a", "reviewer"),
        ("reviewer-b", "reviewer"),
    ] {
        let issued = cli.authed(
            &sup_file,
            &[
                "credential",
                "issue",
                "--actor-name",
                name,
                "--role",
                role,
                "--idempotency-key",
                &format!("f11-issue-{name}"),
            ],
        );
        credentials.push((
            issued["actor_id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap(),
            issued["credential"].as_str().unwrap().to_owned(),
        ));
    }
    let coder_a_id = credentials[0].0;
    let coder_b_id = credentials[1].0;
    let socket = state.join("coordinator.sock");
    let mut coordinator = Coordinator::start(&state);
    wait_for_coordinator(&socket);
    let mut supervisor = Mcp::start(&socket, &sup_token);
    let mut coder_a = Mcp::start(&socket, &credentials[0].1);
    let mut coder_b = Mcp::start(&socket, &credentials[1].1);

    // Supervisor creates a two-repository run and assigns both tasks before either coding session proceeds.
    let run: Run = value(supervisor.call("tesselate.v1.runs.create", json!({"project_id":project.id,"goal":"ship changes across both registered repositories"}), Some("f11-run"))["value"].clone());
    let task_a: Task = value(
        supervisor.call(
            "tesselate.v1.tasks.create",
            json!({"run_id":run.id,"title":"implement repo A","goal":"add a reviewed behavior"}),
            Some("f11-task-a"),
        )["value"]
            .clone(),
    );
    let task_b: Task = value(supervisor.call("tesselate.v1.tasks.create", json!({"run_id":run.id,"title":"implement repo B","goal":"add an independent reviewed behavior"}), Some("f11-task-b"))["value"].clone());
    for (task, coder) in [(&task_a, coder_a_id), (&task_b, coder_b_id)] {
        supervisor.call(
            "tesselate.v1.tasks.assign",
            json!({"task_id":task.id,"assignee":coder}),
            Some(&format!("assign-{}", task.id.0)),
        );
    }
    coder_a.call(
        "tesselate.v1.tasks.claim",
        json!({"task_id":task_a.id}),
        Some("claim-a"),
    );
    coder_b.call(
        "tesselate.v1.tasks.claim",
        json!({"task_id":task_b.id}),
        Some("claim-b"),
    );
    let ws_a: Workspace = value(
        coder_a.call(
            "tesselate.v1.workspaces.request",
            json!({"task_id":task_a.id,"repository_id":repo_a_id,"base_commit":base_a}),
            Some("workspace-a"),
        )["value"]
            .clone(),
    );
    let ws_b: Workspace = value(
        coder_b.call(
            "tesselate.v1.workspaces.request",
            json!({"task_id":task_b.id,"repository_id":repo_b_id,"base_commit":base_b}),
            Some("workspace-b"),
        )["value"]
            .clone(),
    );
    coder_a.call("tesselate.v1.messages.send", json!({"project_id":project.id,"run_id":run.id,"task_id":task_a.id,"body":"repo A workspace ready"}), Some("message-a"));
    coder_b.call("tesselate.v1.messages.send", json!({"project_id":project.id,"run_id":run.id,"task_id":task_b.id,"body":"repo B workspace ready"}), Some("message-b"));
    for (ws, content) in [(&ws_a, "repo-a change\n"), (&ws_b, "repo-b change\n")] {
        fs::write(Path::new(&ws.path).join("feature.txt"), content).unwrap();
        git(Path::new(&ws.path), &["add", "feature.txt"]);
        git(
            Path::new(&ws.path),
            &["commit", "--quiet", "-m", "feature change"],
        );
    }
    let ref_a = CommitRef {
        repository_id: repo_a_id,
        commit_id: git(Path::new(&ws_a.path), &["rev-parse", "HEAD"]),
    };
    let ref_b = CommitRef {
        repository_id: repo_b_id,
        commit_id: git(Path::new(&ws_b.path), &["rev-parse", "HEAD"]),
    };
    coder_a.call(
        "tesselate.v1.workspaces.commits.capture",
        json!({"workspace_id":ws_a.id,"expected_head":ref_a.commit_id}),
        Some("capture-a-1"),
    );
    coder_b.call(
        "tesselate.v1.workspaces.commits.capture",
        json!({"workspace_id":ws_b.id,"expected_head":ref_b.commit_id}),
        Some("capture-b-1"),
    );
    let message_page = coder_a.call(
        "tesselate.v1.messages.read",
        json!({"project_id":project.id,"task_id":task_a.id,"page":{"limit":1}}),
        None,
    );
    let coder_cursor = message_page["next_cursor"].as_str().map(str::to_owned);
    coder_a.call(
        "tesselate.v1.tasks.submit",
        json!({"task_id":task_a.id,"commits":[ref_a],"summary":"repo A initial implementation"}),
        Some("submit-a-1"),
    );
    coder_b.call(
        "tesselate.v1.tasks.submit",
        json!({"task_id":task_b.id,"commits":[ref_b],"summary":"repo B implementation"}),
        Some("submit-b"),
    );
    let round_a: Value = supervisor.call(
        "tesselate.v1.reviews.request",
        json!({"task_id":task_a.id,"commit_refs":[ref_a],"previous_finding_ids":[]}),
        Some("round-a-1"),
    )["value"]
        .clone();
    let round_b: Value = supervisor.call(
        "tesselate.v1.reviews.request",
        json!({"task_id":task_b.id,"commit_refs":[ref_b],"previous_finding_ids":[]}),
        Some("round-b-1"),
    )["value"]
        .clone();

    // Restart with unsubmitted reviews, then reconnect four authenticated sessions and continue the same cursors.
    drop(supervisor);
    drop(coder_a);
    drop(coder_b);
    coordinator.stop();
    coordinator = Coordinator::start(&state);
    wait_for_coordinator(&socket);
    let mut supervisor = Mcp::start(&socket, &sup_token);
    let mut coder_a = Mcp::start(&socket, &credentials[0].1);
    let mut coder_b = Mcp::start(&socket, &credentials[1].1);
    let mut reviewer_a = Mcp::start(&socket, &credentials[2].1);
    let mut reviewer_b = Mcp::start(&socket, &credentials[3].1);
    let persisted: Vec<Task> = value(
        coder_a.call(
            "tesselate.v1.tasks.list",
            json!({"run_id":run.id,"page":{"limit":20}}),
            None,
        )["items"]
            .clone(),
    );
    assert_eq!(persisted.len(), 2);
    let restored_a = persisted.iter().find(|t| t.id == task_a.id).unwrap();
    let restored_b = persisted.iter().find(|t| t.id == task_b.id).unwrap();
    assert_eq!(restored_a.owner, Some(tesselate::ActorId(coder_a_id)));
    assert_eq!(restored_b.owner, Some(tesselate::ActorId(coder_b_id)));
    assert_eq!(restored_a.status, tesselate::TaskStatus::InReview);
    assert_eq!(restored_b.status, tesselate::TaskStatus::InReview);
    let rounds: Value = reviewer_a.call(
        "tesselate.v1.reviews.rounds.list",
        json!({"task_id":task_a.id,"page":{"limit":10}}),
        None,
    );
    assert_eq!(rounds["items"][0]["id"], round_a["id"]);
    assert_eq!(rounds["items"][0]["commit_refs"][0], json!(ref_a));
    assert_eq!(rounds["items"][0]["previous_finding_ids"], json!([]));
    let reviews: Value = reviewer_a.call(
        "tesselate.v1.reviews.list",
        json!({"task_id":task_a.id,"page":{"limit":10}}),
        None,
    );
    assert!(reviews["items"].as_array().unwrap().is_empty());
    let after = coder_a.call("tesselate.v1.messages.read", json!({"project_id":project.id,"task_id":task_a.id,"page":{"limit":10,"after":coder_cursor}}), None);
    assert_eq!(after["items"].as_array().unwrap().len(), 0);
    let all_messages = coder_b.call(
        "tesselate.v1.messages.read",
        json!({"project_id":project.id,"task_id":task_b.id,"page":{"limit":10}}),
        None,
    );
    assert_eq!(all_messages["items"][0]["body"], "repo B workspace ready");
    let inspections: Vec<Value> = [(&mut coder_a, &ws_a), (&mut coder_b, &ws_b)]
        .into_iter()
        .map(|(c, w)| {
            c.call(
                "tesselate.v1.workspaces.inspect",
                json!({"workspace_id":w.id}),
                None,
            )
        })
        .collect();
    assert_eq!(inspections.len(), 2);
    assert_eq!(inspections[0]["head"]["commit_id"], json!(ref_a.commit_id));
    assert_eq!(inspections[1]["head"]["commit_id"], json!(ref_b.commit_id));

    // Both non-author reviewers give explicit decisions. Reviewer A finds a concrete issue on A; B explicitly passes.
    let finding_id = FindingId::new();
    let finding = ReviewFinding {
        id: finding_id,
        severity: tesselate::FindingSeverity::Low,
        location: "feature.txt:1".into(),
        evidence: "the first draft lacks the required marker".into(),
        proposed_correction: "add the marker to the first line".into(),
    };
    reviewer_a.call("tesselate.v1.reviews.submit", json!({"round_id":round_a["id"],"passed":false,"summary":"one actionable finding","findings":[finding]}), Some("review-a-finding"));
    reviewer_b.call(
        "tesselate.v1.reviews.submit",
        json!({"round_id":round_a["id"],"passed":true,"summary":"independent pass","findings":[]}),
        Some("review-a-pass"),
    );
    supervisor.call(
        "tesselate.v1.messages.send",
        json!({"project_id":project.id,"run_id":run.id,"task_id":task_a.id,"body":"Please add the required marker from reviewer-a's finding, then submit a revision."}),
        Some("route-review-a-finding"),
    );
    let routed = coder_a.call(
        "tesselate.v1.messages.read",
        json!({"project_id":project.id,"task_id":task_a.id,"page":{"limit":10,"after":coder_cursor}}),
        None,
    );
    assert_eq!(routed["items"].as_array().unwrap().len(), 1);
    assert!(routed["items"][0]["body"]
        .as_str()
        .unwrap()
        .contains("required marker"));
    for reviewer in [&mut reviewer_a, &mut reviewer_b] {
        reviewer.call("tesselate.v1.reviews.submit", json!({"round_id":round_b["id"],"passed":true,"summary":"independent pass","findings":[]}), Some(&format!("review-b-{}",reviewer.3)));
    }
    supervisor.call("tesselate.v1.reviews.disposition", json!({"task_id":task_a.id,"dispositions":[{"finding_id":finding_id,"disposition":"fixed","reason":"added requested marker"}]}), Some("dispose-finding-a"));
    supervisor.call(
        "tesselate.v1.tasks.assign",
        json!({"task_id":task_a.id,"assignee":coder_a_id}),
        Some("reassign-revision-a"),
    );
    coder_a.call(
        "tesselate.v1.tasks.claim",
        json!({"task_id":task_a.id}),
        Some("claim-revision-a"),
    );
    fs::write(
        Path::new(&ws_a.path).join("feature.txt"),
        "repo-a change with marker\n",
    )
    .unwrap();
    git(Path::new(&ws_a.path), &["add", "feature.txt"]);
    git(
        Path::new(&ws_a.path),
        &["commit", "--quiet", "-m", "address review finding"],
    );
    let ref_a2 = CommitRef {
        repository_id: repo_a_id,
        commit_id: git(Path::new(&ws_a.path), &["rev-parse", "HEAD"]),
    };
    coder_a.call(
        "tesselate.v1.workspaces.commits.capture",
        json!({"workspace_id":ws_a.id,"expected_head":ref_a2.commit_id}),
        Some("capture-a-2"),
    );
    coder_a.call(
        "tesselate.v1.tasks.submit",
        json!({"task_id":task_a.id,"commits":[ref_a2],"summary":"revision addresses finding"}),
        Some("submit-a-2"),
    );
    let round_a2 = supervisor.call(
        "tesselate.v1.reviews.request",
        json!({"task_id":task_a.id,"commit_refs":[ref_a2],"previous_finding_ids":[finding_id]}),
        Some("round-a-2"),
    )["value"]
        .clone();
    let revised_rounds: Value = reviewer_a.call(
        "tesselate.v1.reviews.rounds.list",
        json!({"task_id":task_a.id,"page":{"limit":10}}),
        None,
    );
    assert_eq!(revised_rounds["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        revised_rounds["items"][1]["previous_finding_ids"],
        json!([finding_id])
    );
    for (n, reviewer) in [(&mut reviewer_a, "a"), (&mut reviewer_b, "b")] {
        n.call("tesselate.v1.reviews.submit", json!({"round_id":round_a2["id"],"passed":true,"summary":"revision verified","findings":[]}), Some(&format!("review-a2-{reviewer}")));
    }
    for task in [&task_a, &task_b] {
        supervisor.call(
            "tesselate.v1.tasks.disposition",
            json!({"task_id":task.id,"outcome":"completed","reason":"reviews satisfied"}),
            Some(&format!("complete-task-{}", task.id.0)),
        );
    }
    let complete = supervisor.call("tesselate.v1.runs.complete", json!({"run_id":run.id,"outstanding_integration_steps":["integrate reviewed commits into the project branch after operator review"]}), Some("complete-autonomous-run"));
    let completed: Run = value(complete["value"].clone());
    assert_eq!(completed.status, RunStatus::Completed);
    assert_eq!(completed.reviewed_commits.len(), 2);
    assert!(completed.reviewed_commits.contains(&ref_a2));
    assert!(completed.reviewed_commits.contains(&ref_b));

    // Dirty cleanup is explicitly refused and preserves the worktree.
    fs::write(Path::new(&ws_b.path).join("uncommitted.txt"), "keep me\n").unwrap();
    cli.run_fails(
        &[
            "--credential-file",
            sup_file.to_str().unwrap(),
            "workspace",
            "clean",
            "--workspace-id",
            &ws_b.id.0.to_string(),
            "--idempotency-key",
            "dirty-cleanup",
        ],
        "uncommitted changes",
    );
    assert!(Path::new(&ws_b.path).exists());
    fs::remove_file(Path::new(&ws_b.path).join("uncommitted.txt")).unwrap();
    cli.authed(
        &sup_file,
        &[
            "workspace",
            "clean",
            "--workspace-id",
            &ws_b.id.0.to_string(),
            "--idempotency-key",
            "clean-b",
        ],
    );

    // Portable exports are stable and sufficient to reconstruct the completed run.
    let export1 = cli.authed(
        &sup_file,
        &["artifact", "export", "--run-id", &run.id.0.to_string()],
    );
    let refs = export1["references"].as_array().unwrap();
    let paths: Vec<PathBuf> = refs
        .iter()
        .map(|r| PathBuf::from(r["path"].as_str().unwrap()))
        .collect();
    let event_bytes = fs::read(&paths[0]).unwrap();
    let event_lines: Vec<Value> = event_bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(
        event_lines.len() as u64,
        export1["event_count"].as_u64().unwrap()
    );
    let record_bytes = fs::read(&paths[1]).unwrap();
    let record_lines: Vec<Value> = record_bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(
        record_lines.len() as u64,
        export1["record_count"].as_u64().unwrap()
    );
    assert!(event_lines
        .iter()
        .all(|line| line["export_schema_version"] == 1));
    assert!(record_lines
        .iter()
        .all(|line| line["export_schema_version"] == 1));

    // Reconstruct the run graph only from the two exported JSONL files; this deliberately
    // does not query the service, its database, or the in-memory application.
    let record_values = |kind: &str| -> Vec<&Value> {
        record_lines
            .iter()
            .filter(|line| line["record_type"] == kind)
            .map(|line| &line["record"])
            .collect()
    };
    let task_records = record_values("task");
    for (task, commit) in [(&task_a, &ref_a2), (&task_b, &ref_b)] {
        let record = task_records
            .iter()
            .find(|record| record["task"]["id"] == json!(task.id))
            .expect("export contains the task");
        assert_eq!(record["task"]["run_id"], json!(run.id));
        assert!(record["task"]["submitted_commits"]
            .as_array()
            .unwrap()
            .contains(&json!(commit)));
    }
    let exported_repositories = record_values("repository");
    for repository_id in [repo_a_id, repo_b_id] {
        assert!(exported_repositories
            .iter()
            .any(|record| record["id"] == json!(repository_id)));
    }
    let assignments = record_values("assignment");
    assert!(assignments
        .iter()
        .any(|record| record["task_id"] == json!(task_a.id)
            && record["assignee"] == json!(tesselate::ActorId(coder_a_id))));
    assert!(assignments
        .iter()
        .any(|record| record["task_id"] == json!(task_b.id)
            && record["assignee"] == json!(tesselate::ActorId(coder_b_id))));
    let exported_messages = record_values("message");
    assert!(exported_messages
        .iter()
        .any(|record| record["task_id"] == json!(task_a.id)
            && record["body"]
                .as_str()
                .is_some_and(|body| body.contains("reviewer-a's finding"))));
    let exported_commits = record_values("workspace_commit");
    for commit in [&ref_a, &ref_b, &ref_a2] {
        assert!(exported_commits
            .iter()
            .any(|record| record["commit"] == json!(commit)));
    }
    let rounds = record_values("review_round");
    assert!(rounds
        .iter()
        .any(|record| record["task_id"] == json!(task_a.id)
            && record["round"] == 2
            && record["previous_finding_ids"] == json!([finding_id])));
    let reviews = record_values("review");
    assert!(reviews
        .iter()
        .any(|record| record["task_id"] == json!(task_a.id) && record["passed"] == false));
    assert!(
        reviews
            .iter()
            .filter(|record| record["task_id"] == json!(task_a.id) && record["passed"] == true)
            .count()
            >= 3
    );
    let findings = record_values("review_finding");
    assert!(findings
        .iter()
        .any(|record| record["finding"]["id"] == json!(finding_id)
            && record["finding"]["proposed_correction"]
                .as_str()
                .is_some_and(|text| text.contains("marker"))));
    let dispositions = record_values("finding_disposition");
    assert!(dispositions
        .iter()
        .any(
            |record| record["disposition"]["finding_id"] == json!(finding_id)
                && record["disposition"]["disposition"] == "fixed"
        ));
    let exported_run = record_values("run")
        .into_iter()
        .find(|record| record["id"] == json!(run.id))
        .unwrap();
    assert_eq!(exported_run["status"], "completed");
    assert_eq!(
        exported_run["reviewed_commits"].as_array().unwrap().len(),
        2
    );
    assert!(exported_run["reviewed_commits"]
        .as_array()
        .unwrap()
        .contains(&json!(ref_a2)));
    assert!(exported_run["reviewed_commits"]
        .as_array()
        .unwrap()
        .contains(&json!(ref_b)));
    let event_types: Vec<&str> = event_lines
        .iter()
        .map(|line| line["event"]["event_type"].as_str().unwrap())
        .collect();
    for event_type in [
        "task.assigned",
        "message.sent",
        "task.submitted",
        "review.round_requested",
        "review.submitted",
        "review.findings_dispositioned",
        "task.disposition",
        "run.completed",
    ] {
        assert!(
            event_types.contains(&event_type),
            "export omits {event_type}"
        );
    }
    let markdown = fs::read_to_string(&paths[2]).unwrap();
    assert!(markdown.contains("review.submitted") && markdown.contains("run.completed"));
    assert!(markdown.contains(&run.id.0.to_string()));
    assert!(markdown.contains(&task_a.id.0.to_string()));
    assert!(markdown.contains(&ref_a2.commit_id) && markdown.contains(&ref_b.commit_id));
    let all_artifacts = [
        String::from_utf8_lossy(&event_bytes),
        String::from_utf8_lossy(&record_bytes),
        std::borrow::Cow::Borrowed(markdown.as_str()),
    ];
    for artifact in &all_artifacts {
        assert!(!artifact.contains(&sup_token));
        assert!(!artifact.contains("repo-a change with marker"));
    }
    let digest_before = [
        fs::read(&paths[0]).unwrap(),
        fs::read(&paths[1]).unwrap(),
        fs::read(&paths[2]).unwrap(),
    ];
    cli.authed(
        &sup_file,
        &["artifact", "export", "--run-id", &run.id.0.to_string()],
    );
    assert_eq!(
        digest_before,
        [
            fs::read(&paths[0]).unwrap(),
            fs::read(&paths[1]).unwrap(),
            fs::read(&paths[2]).unwrap()
        ]
    );

    // A separate configuration governs completion; CLI resolution resumes the same transition.
    let gated_config = tmp.0.join("gated.toml");
    fs::write(
        &gated_config,
        format!(
            r#"name="f11-human-gated"
artifact_dir="{}"
[[repositories]]
id="repo-b"
path="{}"
[workflow]
max_duration_hours=24
max_concurrent_assignments=2
max_task_retries=2
max_review_rounds=3
[review]
min_independent_reviewers=2
[human]
completion_approval=true
"#,
            tmp.0.join("gated-artifacts").display(),
            repo_b.display()
        ),
    )
    .unwrap();
    let gated_project: Project = value(
        cli.authed(
            &sup_file,
            &[
                "project",
                "register",
                "--config",
                gated_config.to_str().unwrap(),
                "--idempotency-key",
                "f11-register-gated",
            ],
        )["value"]
            .clone(),
    );
    let gated_run: Run = value(
        supervisor.call(
            "tesselate.v1.runs.create",
            json!({"project_id":gated_project.id,"goal":"human approval demonstration"}),
            Some("f11-gated-run"),
        )["value"]
            .clone(),
    );
    cli.run_fails(
        &[
            "--credential-file",
            sup_file.to_str().unwrap(),
            "call",
            "tesselate.v1.runs.complete",
            "--params",
            &json!({"run_id":gated_run.id,"outstanding_integration_steps":[]}).to_string(),
            "--idempotency-key",
            "gate-first-attempt",
        ],
        "pending",
    );
    let pending: Value = cli.authed(
        &sup_file,
        &[
            "inspect",
            "human",
            "--project-id",
            &gated_project.id.0.to_string(),
        ],
    );
    let request = pending["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["run_id"] == json!(gated_run.id))
        .unwrap();
    cli.authed(
        &sup_file,
        &[
            "human",
            "resolve",
            "--request-id",
            request["id"].as_str().unwrap(),
            "--choice",
            "approve",
            "--reason",
            "accept reviewed outcome",
            "--idempotency-key",
            "f11-approve-gate",
        ],
    );
    let gated_done = supervisor.call(
        "tesselate.v1.runs.complete",
        json!({"run_id":gated_run.id,"outstanding_integration_steps":[]}),
        Some("gate-after-approval"),
    );
    assert_eq!(gated_done["value"]["status"], "completed");

    // A distinct run reaches the configured retry limit and records why unfinished work stopped.
    let limit_run: Run = value(
        supervisor.call(
            "tesselate.v1.runs.create",
            json!({"project_id":project.id,"goal":"retry limit demonstration"}),
            Some("f11-limit-run"),
        )["value"]
            .clone(),
    );
    let limited_task: Task = value(supervisor.call("tesselate.v1.tasks.create",json!({"run_id":limit_run.id,"title":"bounded retries","goal":"stop after configured attempts"}),Some("f11-limit-task"))["value"].clone());
    for attempt in 0..4 {
        if attempt == 3 {
            let error = supervisor.call_error(
                "tesselate.v1.tasks.assign",
                json!({"task_id":limited_task.id,"assignee":coder_a_id}),
                Some(&format!("f11-limit-assign-{attempt}")),
            );
            assert!(error.contains("task retry limit"), "{error}");
            break;
        }
        supervisor.call(
            "tesselate.v1.tasks.assign",
            json!({"task_id":limited_task.id,"assignee":coder_a_id}),
            Some(&format!("f11-limit-assign-{attempt}")),
        );
        let status = coder_a.call(
            "tesselate.v1.tasks.claim",
            json!({"task_id":limited_task.id}),
            Some(&format!("f11-limit-claim-{attempt}")),
        );
        let _ = status;
        if attempt < 3 {
            coder_a.call(
                "tesselate.v1.tasks.release",
                json!({"task_id":limited_task.id,"reason":"exercise retry budget"}),
                Some(&format!("f11-limit-release-{attempt}")),
            );
        }
    }
    let limit_page = supervisor.call(
        "tesselate.v1.runs.list",
        json!({"project_id":project.id,"page":{"limit":100}}),
        None,
    );
    let stopped: Vec<Run> = value(limit_page["items"].clone());
    let limited = stopped.into_iter().find(|r| r.id == limit_run.id).unwrap();
    let remaining_task_ids = &limited.outcome.as_ref().unwrap().remaining_task_ids;
    assert!(remaining_task_ids.contains(&limited_task.id));
    let limit_seen = limited.status == RunStatus::Stopped
        && limited
            .outcome
            .as_ref()
            .is_some_and(|o| o.reason.contains("retry limit"));
    assert!(
        limit_seen,
        "run did not persist a retry-limit outcome: {:?}",
        limited.outcome
    );

    // Exercise readable task/event history after reconnect and leave the coordinator in shared state.
    let timeline = reviewer_a.call("tesselate.v1.timeline.read",json!({"scope":{"project_id":project.id,"run_id":run.id,"task_id":null},"page":{"limit":500}}),None);
    assert!(timeline["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event_type"] == "run.completed"));
    drop(coder_a);
    drop(coder_b);
    drop(reviewer_a);
    drop(reviewer_b);
    drop(supervisor);
    coordinator.stop();
}
