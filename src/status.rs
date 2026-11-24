//! Read-only live status for OMP-coordinated runs.
//!
//! The OMP bridge writes per-run heartbeats under `omp-runs/<run_id>/heartbeat/`.
//! This module renders those heartbeats, falling back to recorded worker state and
//! OMP session activity when a run predates heartbeat support.

use chrono::{DateTime, Local, Utc};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime},
};

const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const FRESH_SECONDS: i64 = 15;
const ACTIVE_MS: u64 = 45_000;
const TAIL_BYTES: u64 = 128 * 1024;

#[derive(Debug, Default, Deserialize)]
struct RunRecord {
    #[serde(default)]
    run_id: String,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    round: u32,
    #[serde(default)]
    spec: SpecRecord,
    #[serde(default)]
    workers: BTreeMap<String, WorkerRecord>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SpecRecord {
    #[serde(default)]
    goal: String,
}

#[derive(Debug, Default, Deserialize)]
struct WorkerRecord {
    #[serde(default)]
    role: String,
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    finished: bool,
    #[serde(default)]
    session_dir: Option<String>,
    #[serde(default)]
    assignment: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Heartbeat {
    #[serde(default)]
    assignment: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    working: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct AgentView {
    role: String,
    assignment: String,
    status: String,
    working: bool,
    order: (u8, u32, u32),
}

#[derive(Debug, Clone)]
struct RunView {
    short: String,
    repository: String,
    phase: String,
    round: u32,
    supervisor: AgentView,
    agents: Vec<AgentView>,
    supervisor_stale: bool,
    error: Option<String>,
    updated: SystemTime,
}

#[derive(Debug)]
struct Activity {
    age_ms: u64,
    tool: Option<String>,
}

/// Run a one-shot snapshot or continuously refresh until interrupted.
pub fn run(state_dir: &Path, watch: bool, interval_ms: u64) -> io::Result<()> {
    let interval = Duration::from_millis(interval_ms.clamp(80, 5_000));
    let stdout = io::stdout();
    let mut frame = 0usize;
    loop {
        let runs = snapshot(state_dir);
        let body = render(&runs, frame, terminal_width());
        let mut out = stdout.lock();
        if watch {
            write!(out, "\x1b[H\x1b[J{body}")?;
            write!(
                out,
                "\nCtrl+C to exit · refresh {}ms · heartbeat {}s / activity {}s\n",
                interval_ms,
                FRESH_SECONDS,
                ACTIVE_MS / 1000
            )?;
        } else {
            write!(out, "{body}")?;
        }
        out.flush()?;
        if !watch {
            return Ok(());
        }
        frame = frame.wrapping_add(1);
        thread::sleep(interval);
    }
}

fn snapshot(state_dir: &Path) -> Vec<RunView> {
    let root = state_dir.join("omp-runs");
    let Ok(entries) = fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut runs = Vec::new();
    for entry in entries.flatten() {
        let dir = entry.path();
        let record_path = dir.join("record.json");
        if !record_path.is_file() {
            continue;
        }
        let Ok(bytes) = fs::read(&record_path) else {
            continue;
        };
        let Ok(record) = serde_json::from_slice::<RunRecord>(&bytes) else {
            continue;
        };
        if record.run_id.is_empty() {
            continue;
        }
        runs.push(build_run(&dir, record));
    }
    runs.sort_by_key(|run| std::cmp::Reverse(run.updated));
    runs
}

fn build_run(dir: &Path, record: RunRecord) -> RunView {
    let heartbeat_dir = dir.join("heartbeat");
    let supervisor_heartbeat = read_heartbeat(&heartbeat_dir.join("supervisor.json"));
    let supervisor_fresh = supervisor_heartbeat
        .as_ref()
        .is_some_and(|heartbeat| is_fresh(heartbeat.updated_at.as_deref()));
    let goal = nonempty(&record.spec.goal).unwrap_or_else(|| "(no goal)".into());
    let supervisor_assignment = shorten(&goal, 80);
    let mut heartbeat_error = None;
    let (supervisor, supervisor_stale) = match supervisor_heartbeat.as_ref().filter(|_| supervisor_fresh) {
        Some(heartbeat) => {
            heartbeat_error = heartbeat.error.clone();
            (
                AgentView {
                    role: "supervisor".into(),
                    assignment: nonempty(&heartbeat.assignment).unwrap_or(supervisor_assignment.clone()),
                    status: heartbeat_status(heartbeat, &record),
                    working: heartbeat.working,
                    order: (0, 0, 0),
                },
                false,
            )
        }
        None => {
            let done = record.phase == "done";
            (
                AgentView {
                    role: "supervisor".into(),
                    assignment: supervisor_assignment,
                    status: if done {
                        "complete".into()
                    } else {
                        "no supervisor heartbeat".into()
                    },
                    working: false,
                    order: (0, 0, 0),
                },
                !done,
            )
        }
    };
    let mut agents: Vec<AgentView> = record
        .workers
        .iter()
        .map(|(label, worker)| {
            let heartbeat = read_heartbeat(&heartbeat_dir.join(format!("{label}.json")));
            let fresh = heartbeat
                .as_ref()
                .is_some_and(|heartbeat| is_fresh(heartbeat.updated_at.as_deref()));
            let heartbeat = heartbeat.as_ref().filter(|_| fresh);
            let assignment = heartbeat
                .and_then(|heartbeat| nonempty(&heartbeat.assignment))
                .or_else(|| nonempty(worker.assignment.as_deref().unwrap_or("")))
                .unwrap_or_else(|| fallback_assignment(label));
            let (status, working) = match heartbeat {
                Some(heartbeat) => (
                    if heartbeat.status.is_empty() {
                        heartbeat.state.clone()
                    } else {
                        heartbeat.status.clone()
                    },
                    heartbeat.working,
                ),
                None if worker.finished => ("finished".into(), false),
                None => worker_fallback_status(worker).unwrap_or_else(|| ("process ended".into(), false)),
            };
            AgentView {
                role: display_role(label, &worker.role),
                assignment,
                status,
                working,
                order: label_order(label),
            }
        })
        .collect();
    agents.sort_by_key(|agent| agent.order);
    let updated = newest_time([
        dir.join("record.json").metadata().and_then(|m| m.modified()).ok(),
        dir.join("heartbeat")
            .metadata()
            .and_then(|m| m.modified())
            .ok(),
    ]);
    RunView {
        short: record.run_id.chars().take(8).collect(),
        repository: record.repository,
        phase: record.phase,
        round: record.round,
        supervisor,
        agents,
        supervisor_stale,
        error: heartbeat_error.or(record.error),
        updated,
    }
}

fn heartbeat_status(heartbeat: &Heartbeat, record: &RunRecord) -> String {
    if !heartbeat.status.is_empty() {
        return heartbeat.status.clone();
    }
    match heartbeat.state.as_str() {
        "finished" => "complete".into(),
        "failed" => "failed".into(),
        "stopped" => "stopped".into(),
        _ => format!("{} · round {}", record.phase, record.round),
    }
}

fn worker_fallback_status(worker: &WorkerRecord) -> Option<(String, bool)> {
    if !pid_alive(worker.pid) {
        return None;
    }
    match worker_activity(worker.session_dir.as_deref()) {
        Some(activity) => Some(activity_status(&activity)),
        None => Some(("starting".into(), true)),
    }
}

fn worker_activity(session_dir: Option<&str>) -> Option<Activity> {
    let directory = Path::new(session_dir?);
    let entries = fs::read_dir(directory).ok()?;
    let mut newest: Option<(PathBuf, SystemTime, u64)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let metadata = entry.metadata().ok()?;
        let modified = metadata.modified().ok()?;
        if newest
            .as_ref()
            .is_none_or(|(_, current, _)| modified > *current)
        {
            newest = Some((path, modified, metadata.len()));
        }
    }
    let (path, modified, size) = newest?;
    let age_ms = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default()
        .as_millis() as u64;
    Some(Activity {
        age_ms,
        tool: last_tool(&path, size),
    })
}

fn last_tool(path: &Path, size: u64) -> Option<String> {
    let mut file = fs::File::open(path).ok()?;
    let start = size.saturating_sub(TAIL_BYTES);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut buffer = Vec::with_capacity(size.saturating_sub(start) as usize);
    file.read_to_end(&mut buffer).ok()?;
    let text = String::from_utf8_lossy(&buffer);
    let marker = "\"toolName\":\"";
    let index = text.rfind(marker)?;
    let rest = &text[index + marker.len()..];
    let end = rest.find('"')?;
    let tool = &rest[..end];
    (!tool.is_empty() && tool.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
        .then(|| tool.to_owned())
}

fn activity_status(activity: &Activity) -> (String, bool) {
    let phrase = activity
        .tool
        .as_deref()
        .map(tool_phrase)
        .unwrap_or("thinking");
    if activity.age_ms <= ACTIVE_MS {
        (format!("{phrase} · {}", ago(activity.age_ms)), true)
    } else {
        (format!("quiet · {}", ago(activity.age_ms)), false)
    }
}

fn tool_phrase(tool: &str) -> &'static str {
    match tool {
        "edit" | "ast_edit" => "editing files",
        "write" => "writing files",
        "bash" => "running commands",
        "read" => "reading code",
        "grep" => "searching code",
        "glob" => "searching files",
        "task" => "delegating work",
        "ask" => "awaiting input",
        "web_search" => "researching",
        _ => "working",
    }
}

fn ago(ms: u64) -> String {
    let seconds = ms / 1000;
    if seconds < 5 {
        "just now".into()
    } else if seconds < 60 {
        format!("{seconds}s ago")
    } else if seconds < 3_600 {
        format!("{}m ago", seconds / 60)
    } else {
        format!("{}h ago", seconds / 3_600)
    }
}

fn pid_alive(pid: Option<u32>) -> bool {
    let Some(pid) = pid else {
        return false;
    };
    #[cfg(unix)]
    {
        pid > 0 && unsafe { libc::kill(pid as i32, 0) } == 0
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn display_role(label: &str, role: &str) -> String {
    if let Some(rest) = label.strip_prefix("reviewer-") {
        if let Some((index, _)) = rest.split_once("-round-") {
            if let Ok(index) = index.parse::<u32>() {
                return format!("reviewer {}", index + 1);
            }
        }
    }
    if label.starts_with("coder-") {
        return "coder".into();
    }
    nonempty(role).unwrap_or_else(|| label.into())
}

fn fallback_assignment(label: &str) -> String {
    if let Some(round) = label.strip_prefix("coder-round-") {
        return format!("implementation · round {round}");
    }
    if let Some(rest) = label.strip_prefix("reviewer-") {
        if let Some((index, round)) = rest.split_once("-round-") {
            let index = index.parse::<u32>().map(|value| value + 1).unwrap_or(1);
            return format!("review · round {round} · reviewer {index}/2");
        }
    }
    label.into()
}

fn label_order(label: &str) -> (u8, u32, u32) {
    if let Some(round) = label.strip_prefix("coder-round-") {
        return (0, round.parse().unwrap_or(0), 0);
    }
    if let Some(rest) = label.strip_prefix("reviewer-") {
        if let Some((index, round)) = rest.split_once("-round-") {
            return (1, round.parse().unwrap_or(0), index.parse().unwrap_or(0));
        }
    }
    (2, 0, 0)
}

fn read_heartbeat(path: &Path) -> Option<Heartbeat> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn is_fresh(updated_at: Option<&str>) -> bool {
    let Some(updated_at) = updated_at else {
        return false;
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(updated_at) else {
        return false;
    };
    let age = Utc::now().signed_duration_since(parsed.with_timezone(&Utc));
    age.num_seconds() <= FRESH_SECONDS
}

fn render(runs: &[RunView], frame: usize, width: usize) -> String {
    let width = width.max(40);
    let mut output = String::new();
    output.push_str(&format!(
        "Tesselate status · {} run{} · {}\n",
        runs.len(),
        if runs.len() == 1 { "" } else { "s" },
        Local::now().format("%H:%M:%S")
    ));
    if runs.is_empty() {
        output.push_str("No OMP runs under the configured state directory.\n");
        return output;
    }
    let spinner = SPINNER[frame % SPINNER.len()];
    for run in runs {
        output.push('\n');
        let mut header = format!("Run {} · {}", run.short, basename(&run.repository));
        if !run.phase.is_empty() {
            header.push_str(&format!(" · {}", run.phase));
            if run.round > 0 {
                header.push_str(&format!(" round {}", run.round));
            }
        }
        if run.supervisor_stale {
            header.push_str(" · supervisor stale");
        }
        output.push_str(&truncate(&header, width));
        output.push('\n');
        let role_width = std::iter::once(run.supervisor.role.as_str())
            .chain(run.agents.iter().map(|agent| agent.role.as_str()))
            .map(|role| role.chars().count())
            .max()
            .unwrap_or(8)
            .clamp(8, 14);
        let assignment_width = ((width.saturating_sub(role_width + 4)) * 55 / 100).clamp(18, 58);
        let status_width = width
            .saturating_sub(role_width + assignment_width + 4)
            .max(8);
        output.push_str(&agent_line(
            &run.supervisor,
            spinner,
            role_width,
            assignment_width,
            status_width,
        ));
        output.push('\n');
        for agent in &run.agents {
            output.push_str(&agent_line(
                agent,
                spinner,
                role_width,
                assignment_width,
                status_width,
            ));
            output.push('\n');
        }
        if let Some(error) = &run.error {
            let mut line = format!("  error · {}", shorten(error, 200));
            line = truncate(&line, width);
            output.push_str(&line);
            output.push('\n');
        }
    }
    output
}

fn agent_line(
    agent: &AgentView,
    spinner: &str,
    role_width: usize,
    assignment_width: usize,
    status_width: usize,
) -> String {
    let cursor = if agent.working { spinner } else { "·" };
    let role = pad(&truncate(&agent.role, role_width), role_width);
    let assignment = pad(&truncate(&agent.assignment, assignment_width), assignment_width);
    let status = truncate(&agent.status, status_width.max(1));
    format!("{cursor} {role} {assignment} {status}")
}

fn pad(text: &str, width: usize) -> String {
    let length = text.chars().count();
    if length >= width {
        text.to_owned()
    } else {
        format!("{text}{}", " ".repeat(width - length))
    }
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    if width <= 1 {
        return "…".into();
    }
    let mut shortened: String = text.chars().take(width - 1).collect();
    shortened.push('…');
    shortened
}

fn shorten(text: &str, width: usize) -> String {
    let one_line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate(&one_line, width)
}

fn basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
        .to_owned()
}

fn nonempty(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.trim().to_owned())
}

fn newest_time(times: [Option<SystemTime>; 2]) -> SystemTime {
    times
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(SystemTime::UNIX_EPOCH)
}

fn terminal_width() -> usize {
    #[cfg(unix)]
    {
        let mut size: libc::winsize = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) };
        if result == 0 && size.ws_col > 0 {
            return size.ws_col as usize;
        }
    }
    100
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("tesselate-status-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_record(dir: &Path, workers: &str) {
        let record = format!(
            r#"{{"version":1,"run_id":"12345678-90ab-4cde-8f01-234567890abc","repository":"/tmp/demo","branch":"tesselate/demo","phase":"coding","round":2,"spec":{{"goal":"Make idle units return after combat"}},"workers":{{{workers}}},"error":null}}"#
        );
        fs::write(dir.join("record.json"), record).unwrap();
    }

    fn heartbeat(role: &str, assignment: &str, status: &str, state: &str, working: bool, updated: &str) -> String {
        format!(
            r#"{{"version":1,"run_id":"12345678-90ab-4cde-8f01-234567890abc","agent":"x","role":"{role}","assignment":"{assignment}","status":"{status}","state":"{state}","pid":1,"started_at":"{updated}","updated_at":"{updated}","working":{working}}}"#
        )
    }

    #[test]
    fn fresh_heartbeats_and_spinner_render() {
        let temp = TempDir::new();
        let run = temp.0.join("omp-runs/12345678-90ab-4cde-8f01-234567890abc");
        fs::create_dir_all(run.join("heartbeat")).unwrap();
        write_record(
            &run,
            r#""coder-round-2":{"role":"coder","pid":999999,"finished":false,"assignment":"revise return behavior · round 2"},"reviewer-1-round-2":{"role":"reviewer","pid":999999,"finished":true}"#,
        );
        let now = Utc::now().to_rfc3339();
        fs::write(
            run.join("heartbeat/supervisor.json"),
            heartbeat("supervisor", "Make idle units return after combat", "coding · round 2", "working", true, &now),
        )
        .unwrap();
        fs::write(
            run.join("heartbeat/coder-round-2.json"),
            heartbeat("coder", "revise return behavior · round 2", "editing files · 2s ago", "working", true, &now),
        )
        .unwrap();
        let runs = snapshot(&temp.0);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].phase, "coding");
        assert!(runs[0].supervisor.working);
        assert_eq!(runs[0].agents.len(), 2);
        assert!(runs[0].agents[0].working);
        assert_eq!(runs[0].agents[0].role, "coder");
        assert_eq!(runs[0].agents[1].role, "reviewer 2");
        assert_eq!(runs[0].agents[1].status, "finished");
        let rendered = render(&runs, 0, 120);
        assert!(rendered.contains("Run 12345678 · demo · coding round 2"));
        assert!(rendered.contains("⠋ supervisor"));
        assert!(rendered.contains("⠋ coder"));
        assert!(rendered.contains("editing files · 2s ago"));
        for line in rendered.lines() {
            assert!(line.chars().count() <= 120);
        }
    }

    #[test]
    fn stale_supervisor_falls_back_to_session_activity() {
        let temp = TempDir::new();
        let run = temp.0.join("omp-runs/12345678-90ab-4cde-8f01-234567890abc");
        let sessions = run.join("sessions/coder-round-2");
        fs::create_dir_all(run.join("heartbeat")).unwrap();
        fs::create_dir_all(&sessions).unwrap();
        fs::write(
            sessions.join("session.jsonl"),
            "{\"type\":\"message\",\"message\":{\"toolName\":\"edit\"}}\n",
        )
        .unwrap();
        write_record(
            &run,
            &format!(
                r#""coder-round-2":{{"role":"coder","pid":{},"finished":false,"session_dir":"{}"}}"#,
                std::process::id(),
                sessions.display()
            ),
        );
        let stale = (Utc::now() - chrono::Duration::minutes(5)).to_rfc3339();
        fs::write(
            run.join("heartbeat/supervisor.json"),
            heartbeat("supervisor", "Make idle units return after combat", "coding · round 2", "working", true, &stale),
        )
        .unwrap();
        let runs = snapshot(&temp.0);
        assert!(runs[0].supervisor_stale);
        assert!(!runs[0].supervisor.working);
        assert_eq!(runs[0].supervisor.status, "no supervisor heartbeat");
        assert_eq!(runs[0].agents.len(), 1);
        assert!(runs[0].agents[0].working);
        assert!(runs[0].agents[0].status.starts_with("editing files"));
    }

    #[test]
    fn missing_state_is_empty() {
        let temp = TempDir::new();
        assert!(snapshot(&temp.0).is_empty());
        let rendered = render(&[], 3, 80);
        assert!(rendered.contains("No OMP runs"));
        assert!(rendered.contains("0 runs"));
    }
}
