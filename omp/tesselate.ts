import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative } from "node:path";
import type { ExtensionAPI } from "@oh-my-pi/pi-coding-agent";

type Config = { binary: string; state_dir: string; workspace_root: string; supervisor_credential_file: string };
type Spec = { goal: string; acceptance_criteria: string; constraints: string; validation_command: string; base_branch: string };
type Worker = { role: "coder" | "reviewer"; cwd: string; credential_file: string; log: string; session_dir: string; pid?: number; finished?: boolean };
type Review = { passed: boolean; summary: string; findings: Array<{ severity: "low" | "medium" | "high" | "critical"; location: string; evidence: string; proposed_correction: string }> };
type RunSummary = { id: string; status: string; goal: string };
type TaskState = { id: string; status: string };
type ReviewState = Review & { round_id: string; reviewer: string; findings: Array<Review["findings"][number] & { id: string }> };
type RoundState = { id: string; round: number };
type WorkspaceState = { id: string; path: string };
type CredentialState = { actor_id: string; session_id: string; credential_file: string | null };
type Page<T> = { items: T[]; next_cursor: string | null };
type RunRecord = {
  version: 1; project_id: string; repository_id: string; repository: string; run_id: string; task_id: string;
  base: string; base_branch: string; branch: string; integration_dir: string; workspace_id: string;
  workspace_dir: string; coder_credential_file: string; coder_actor_id: string; reviewer_credential_files: string[]; reviewer_actor_ids: string[];
  spec: Spec; phase: "coding" | "review" | "revision" | "integrating" | "done";
  round: number; last_head?: string; commit_refs?: Array<{ repository_id: string; commit_id: string }>;
  round_id?: string; previous_finding_ids: string[]; workers: Record<string, Worker>;
  validation_log?: string; validation_attempts?: number; validation_passed?: boolean; error?: string;
};
type RunIntent = { version: 1; repository: string; project_id: string; repository_id: string; spec: Spec; base: string; key: string; supervisor_credential_file: string; run_id?: string };
type UIOps = { input(question: string): Promise<string | undefined>; select(question: string, choices: string[]): Promise<string | undefined>; confirm(question: string): Promise<boolean>; notify(message: string, level: "info" | "warning" | "error"): void };

function configAt(path: string): Config {
  let config: unknown;
  try { config = JSON.parse(readFileSync(path, "utf8")); }
  catch { throw new Error("Tesselate OMP configuration is missing or unreadable. Run `tesselate install omp` again."); }
  if (!config || typeof config !== "object") throw new Error("Invalid Tesselate OMP configuration; reinstall with `tesselate install omp`.");
  const c = config as Record<string, unknown>;
  for (const key of ["binary", "state_dir", "workspace_root", "supervisor_credential_file"]) {
    if (typeof c[key] !== "string" || !isAbsolute(c[key] as string)) throw new Error(`Invalid ${key} in Tesselate OMP configuration; reinstall the extension.`);
  }
  return c as Config;
}
function inside(path: string, root: string): boolean {
  const sub = relative(root, path);
  return sub !== "" && sub !== ".." && !sub.startsWith(`..${requireSeparator()}`) && !isAbsolute(sub);
}
function requireSeparator(): string { return process.platform === "win32" ? "\\" : "/"; }
function cleanError(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  return text.replace(/\b(credential|token|secret)=[^\s]+/gi, "$1=[redacted]").slice(0, 1200);
}
function save(record: RunRecord, config: Config): void {
  const dir = join(config.state_dir, "omp-runs", record.run_id);
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  const pending = join(dir, `record.${randomUUID()}.tmp`);
  writeFileSync(pending, JSON.stringify(record, null, 2), { mode: 0o600, flag: "wx" });
  renameSync(pending, join(dir, "record.json"));
}
function load(config: Config, id: string): RunRecord | undefined {
  try {
    const record = JSON.parse(readFileSync(join(config.state_dir, "omp-runs", id, "record.json"), "utf8")) as RunRecord;
    if (record.version !== 1 || record.run_id !== id) throw new Error("unsupported run record");
    return record;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}
function intentPath(config: Config, repository: string): string {
  const digest = createHash("sha256").update(repository).digest("hex");
  return join(config.state_dir, "omp-runs", "intents", `${digest}.json`);
}
function readIntent(config: Config, repository: string): RunIntent | undefined {
  try {
    const intent = JSON.parse(readFileSync(intentPath(config, repository), "utf8")) as RunIntent;
    if (intent.version !== 1 || intent.repository !== repository) throw new Error("Unsupported or mismatched OMP run intent.");
    return intent;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}
function saveIntent(config: Config, intent: RunIntent, create = false): void {
  const path = intentPath(config, intent.repository);
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  if (create) {
    writeFileSync(path, JSON.stringify(intent), { mode: 0o600, flag: "wx" });
    return;
  }
  const pending = `${path}.${randomUUID()}.tmp`;
  writeFileSync(pending, JSON.stringify(intent), { mode: 0o600, flag: "wx" });
  renameSync(pending, path);
}
function credentialPath(config: Config, issued: CredentialState): string {
  if (!/^[0-9a-f-]{36}$/i.test(issued.session_id) || !issued.actor_id) throw new Error("Invalid role credential response.");
  const path = issued.credential_file ?? join(config.state_dir, "credentials", `${issued.session_id}.token`);
  const directory = join(config.state_dir, "credentials");
  if (!isAbsolute(path) || !inside(path, directory)) throw new Error("Role credential path escapes the Tesselate state directory.");
  const metadata = lstatSync(path);
  if (!metadata.isFile() || (metadata.mode & 0o077) !== 0) throw new Error("Role credential file is not private; preserve run for manual recovery.");
  return path;
}
function runProcess(binary: string, args: string[], cwd?: string, env?: NodeJS.ProcessEnv): Promise<string> {
  const { promise, resolve: accept, reject } = Promise.withResolvers<string>();
  const child = spawn(binary, args, { cwd, env: env ?? process.env, stdio: ["ignore", "pipe", "pipe"] });
  let out = ""; let err = "";
  child.stdout!.on("data", (chunk: Buffer) => { out += chunk; });
  child.stderr!.on("data", (chunk: Buffer) => { err += chunk; });
  child.on("error", reject);
  child.on("close", code => code === 0 ? accept(out.trim()) : reject(new Error(`${binary} exited ${code}: ${err.slice(-1200)}`)));
  return promise;
}
async function git(repo: string, ...args: string[]): Promise<string> {
  return runProcess("git", ["-C", repo, ...args]);
}
async function cli<T = Record<string, unknown>>(config: Config, credential: string, command: string[], params?: unknown, key?: string): Promise<T> {
  const args = ["--state-dir", config.state_dir, "--credential-file", credential, ...command];
  if (params !== undefined) args.push("--params", JSON.stringify(params));
  if (key !== undefined) args.push("--idempotency-key", key);
  const response: unknown = JSON.parse(await runProcess(config.binary, args));
  if (response === null || typeof response !== "object") throw new Error("Unexpected Tesselate CLI response.");
  // Credential issuance prints the secret as well as the private file path. Do not retain or log it.
  delete (response as Record<string, unknown>).credential;
  return response as T;
}
async function call<T = Record<string, unknown>>(config: Config, credential: string, op: string, params: unknown, key?: string): Promise<T> {
  return cli<T>(config, credential, ["call", `tesselate.v1.${op}`], params, key);
}
function value<T>(result: { value: T }): T {
  if (result.value === undefined) throw new Error("Tesselate mutation response is missing its value.");
  return result.value;
}
async function listed<T = Record<string, unknown>>(config: Config, credential: string, op: string, params: Record<string, unknown>): Promise<T[]> {
  let after: string | null = null; const items: T[] = [];
  do {
    const response = await call<Page<T>>(config, credential, op, { ...params, page: { limit: 100, after } });
    if (!Array.isArray(response.items) || (response.next_cursor !== null && typeof response.next_cursor !== "string")) throw new Error("Unexpected Tesselate page.");
    items.push(...response.items);
    after = response.next_cursor;
  } while (after);
  return items;
}
function mutationKey(record: RunRecord, action: string): string { return `omp:${record.run_id}:${action}`; }
async function mutate<T = Record<string, unknown>>(config: Config, record: RunRecord, op: string, params: unknown, action: string, credential = config.supervisor_credential_file): Promise<T> {
  return value(await call<{ value: T }>(config, credential, op, params, mutationKey(record, action)));
}
async function repoFor(cwd: string, config: Config): Promise<string> {
  const root = await git(cwd, "rev-parse", "--show-toplevel");
  const canonical = await runProcess("realpath", [root]);
  const workspace = await runProcess("realpath", [config.workspace_root]);
  if (!inside(canonical, workspace)) throw new Error("Open OMP inside a Git repository beneath the configured workspace root.");
  // Never accidentally register a task checkout as a separate project.
  if (await git(canonical, "rev-parse", "--is-shallow-repository") === "true") throw new Error("Shallow checkouts are not supported for integration bases.");
  return canonical;
}
async function chooseRepo(cwd: string, config: Config, ui: UIOps): Promise<string> {
  try { return await repoFor(cwd, config); }
  catch {
    const candidates = readdirSync(config.workspace_root, { withFileTypes: true })
      .filter(item => item.isDirectory() && existsSync(join(config.workspace_root, item.name, ".git")))
      .map(item => item.name);
    const selection = candidates.length
      ? await ui.select("Which repository under the workspace root should own this feature?", [...candidates, "Enter another repository path"])
      : "Enter another repository path";
    if (!selection) throw new Error("No repository selected.");
    const choice = selection === "Enter another repository path"
      ? await ui.input("Absolute path of the local Git repository to use")
      : selection;
    if (!choice) throw new Error("No repository selected.");
    return repoFor(candidates.includes(choice) ? join(config.workspace_root, choice) : choice, config);
  }
}
async function selectBase(repo: string, ui: UIOps): Promise<{ branch: string; base: string }> {
  if (await git(repo, "status", "--porcelain")) throw new Error("The starting checkout has uncommitted changes; choose a clean repository checkout.");
  const branches = (await git(repo, "for-each-ref", "--format=%(refname:short)", "refs/heads")).split("\n").filter(Boolean);
  const branch = await ui.select("Select the local branch to base the new integration branch on", branches);
  if (!branch) throw new Error("No integration base selected.");
  const base = await git(repo, "rev-parse", "--verify", `${branch}^{commit}`);
  if (repo.endsWith("/bitwars")) {
    if (branch !== "main") throw new Error("Bitwars integration must be based on local main. Sync it deliberately before retrying.");
    const upstream = await git(repo, "rev-parse", "--abbrev-ref", "main@{upstream}").catch(() => "");
    if (!upstream) throw new Error("Bitwars main has no upstream. Verify/sync main before starting.");
    const divergence = await git(repo, "rev-list", "--left-right", "--count", `${upstream}...main`);
    if (divergence !== "0\t0") throw new Error("Bitwars main is ahead/diverged or behind upstream; sync explicitly before starting.");
  }
  return { branch, base };
}
function ensureWorkerEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env, TESSELATE_WORKER: "1" };
  delete env.TESSELATE_CREDENTIAL_FILE;
  delete env.TESSELATE_SUPERVISOR_CREDENTIAL_FILE;
  return env;
}
function workerResult(log: string): string {
  const text = readFileSync(log, "utf8").trim();
  if (!text) throw new Error(`Worker did not produce a final response (${log}).`);
  return text;
}
function jsonResult<T>(text: string): T {
  const trimmed = text.trim();
  const match = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  return JSON.parse(match ? match[1] : trimmed) as T;
}
function running(worker: Worker): boolean {
  if (!worker.pid) return false;
  try { process.kill(worker.pid, 0); return true; } catch { return false; }
}
const liveWorkers = new Map<number, Promise<number | null>>();
async function launch(record: RunRecord, config: Config, label: string, role: Worker["role"], cwd: string, credential: string, prompt: string): Promise<string> {
  const prior = record.workers[label];
  if (prior?.finished) return workerResult(prior.log);
  if (prior && !prior.pid) {
    throw new Error(`Worker ${label} has a persisted launch intent but no PID. Inspect ${prior.session_dir} and ${prior.log}; do not relaunch until its status is reconciled.`);
  }
  if (prior && !running(prior)) {
    // A crashed supervisor cannot know whether an orphan exited successfully; the final
    // response and all Git/Tesselate evidence are rechecked by the caller below.
    return workerResult(prior.log);
  }
  if (!prior) {
    const root = join(config.state_dir, "omp-runs", record.run_id);
    const session_dir = join(root, `session-${label}`);
    mkdirSync(session_dir, { recursive: true, mode: 0o700 });
    const log = join(root, `${label}.log`);
    const worker: Worker = { role, cwd, credential_file: credential, log, session_dir };
    record.workers[label] = worker;
    save(record, config); // The launch intent precedes both the log and the subprocess.
    const fd = openSync(log, "wx", 0o600);
    const args = ["-p", "--cwd", cwd, "--session-dir", session_dir, "--wait-async", "--max-time", "2h", "--no-extensions", "--no-skills"];
    if (role === "reviewer") args.push("--tools=read,grep,glob");
    args.push(prompt);
    const child = spawn("omp", args, { cwd, env: ensureWorkerEnv(), detached: true, stdio: ["ignore", fd, fd] });
    closeSync(fd);
    const { promise, resolve: settle, reject } = Promise.withResolvers<number | null>();
    child.once("error", reject);
    child.once("close", settle);
    if (!child.pid) throw new Error("OMP worker failed to spawn.");
    liveWorkers.set(child.pid, promise);
    worker.pid = child.pid;
    save(record, config);
    child.unref();
  }
  const worker = record.workers[label];
  const live = worker.pid && liveWorkers.get(worker.pid);
  if (live) {
    const code = await live;
    liveWorkers.delete(worker.pid!);
    if (code !== 0) throw new Error(`OMP ${label} exited ${code}; inspect private log ${worker.log} before resuming.`);
  } else {
    while (running(worker)) await new Promise(done => setTimeout(done, 3000));
  }
  const result = workerResult(worker.log);
  worker.finished = true; save(record, config);
  return result;
}
async function capture(config: Config, record: RunRecord): Promise<void> {
  const inspection = await call<{ clean: boolean; workspace: { path: string }; head: { commit_id: string } }>(config, config.supervisor_credential_file, "workspaces.inspect", { workspace_id: record.workspace_id });
  if (!inspection.clean || inspection.workspace.path !== record.workspace_dir) throw new Error("Coder workspace is dirty or changed identity; preserve it and reconcile manually.");
  const head = inspection.head.commit_id as string;
  if (head === record.last_head) throw new Error("Coder returned no new committed work; preserve workspace for recovery.");
  await git(record.repository, "merge-base", "--is-ancestor", record.base, head);
  const commits = await mutate<Array<{ repository_id: string; commit_id: string }>>(config, record, "workspaces.commits.capture", { workspace_id: record.workspace_id, expected_head: head }, `capture:${record.round}`);
  if (!Array.isArray(commits) || !commits.length || commits.some(ref => ref.repository_id !== record.repository_id || !/^[0-9a-f]{40,64}$/.test(ref.commit_id))) {
    throw new Error("No valid captured Tesselate commits; cannot submit unverified refs.");
  }
  record.commit_refs = commits;
  record.last_head = head; save(record, config);
}
async function reviewerCheckout(record: RunRecord, config: Config, index: number): Promise<string> {
  const path = join(config.state_dir, "omp-runs", record.run_id, `reviewer-${index}-round-${record.round}`);
  if (!existsSync(path)) await git(record.repository, "worktree", "add", "--detach", path, record.last_head!);
  if (await git(path, "rev-parse", "HEAD") !== record.last_head || await git(path, "status", "--porcelain")) {
    throw new Error(`Reviewer checkout changed or dirty (${path}); preserve for recovery.`);
  }
  return path;
}
function reviewValid(review: Review): boolean {
  return typeof review?.summary === "string" && review.summary.trim().length > 0 && typeof review.passed === "boolean" &&
    Array.isArray(review.findings) && review.passed === (review.findings.length === 0) &&
    review.findings.every(f => ["low", "medium", "high", "critical"].includes(f.severity) &&
      [f.location, f.evidence, f.proposed_correction].every(s => typeof s === "string" && s.trim().length > 0));
}
async function performReviews(config: Config, record: RunRecord): Promise<Review[]> {
  const reviews: Review[] = [];
  for (let i = 0; i < 2; i++) {
    const recorded = (await listed<ReviewState>(config, config.supervisor_credential_file, "reviews.list", { task_id: record.task_id }))
      .find(r => r.round_id === record.round_id && r.reviewer === record.reviewer_actor_ids[i]);
    if (recorded) {
      await reviewerCheckout(record, config, i);
      reviews.push({ passed: recorded.passed, summary: "Previously submitted independent review", findings: recorded.findings });
      continue;
    }
    const cwd = await reviewerCheckout(record, config, i);
    const diff = join(config.state_dir, "omp-runs", record.run_id, `round-${record.round}.diff`);
    if (!existsSync(diff)) writeFileSync(diff, await git(record.repository, "diff", record.base, record.last_head!), { mode: 0o600, flag: "wx" });
    const prompt = `Independent reviewer ${i + 1} for Tesselate task ${record.task_id}, review round ${record.round}. Read AGENTS.md and the diff at ${diff}; inspect relevant sources in this detached checkout. Goal: ${record.spec.goal}\nAcceptance: ${record.spec.acceptance_criteria}\nConstraints: ${record.spec.constraints}\nDo not edit files, invoke shell, run tests, access credentials or shared services. Reply ONLY JSON {"passed":boolean,"summary":"...","findings":[{"severity":"low|medium|high|critical","location":"path:line","evidence":"specific evidence","proposed_correction":"specific fix"}]}. A pass requires no findings; a failure requires findings. This is an adversarial review, not a summary.`;
    const text = await launch(record, config, `reviewer-${i}-round-${record.round}`, "reviewer", cwd, record.reviewer_credential_files[i], prompt);
    const review = jsonResult<Review>(text);
    if (!reviewValid(review)) throw new Error(`Reviewer ${i + 1} returned an invalid review; retain session for recovery.`);
    await reviewerCheckout(record, config, i); // re-check immutable HEAD immediately before accepting result
    const findings = review.findings.map(f => ({ id: randomUUID(), ...f }));
    await mutate(config, record, "reviews.submit", { round_id: record.round_id, ...review, findings }, `review:${record.round}:${i}`, record.reviewer_credential_files[i]);
    reviews.push(review); save(record, config);
  }
  return reviews;
}
async function validate(record: RunRecord, config: Config): Promise<void> {
  const command = record.spec.validation_command.trim();
  if (!command || /[;&|`$<>\n\r]/.test(command) ||
      /\b(?:git\s+(?:push|merge|checkout|switch|reset)|redis-cli|pnpm\s+dev|docker\s+compose\s+up)\b/i.test(command) ||
      (/^git\s/i.test(command) && !/^git\s+(?:diff|status|log|show|rev-parse)\b/i.test(command)) ||
      /^(?:gh|hub|workmux|ssh|rsync|curl|wget|docker|bash|sh)\b/i.test(command)) {
    throw new Error("Choose one safe, explicit integration-only validation command (no shell chaining, push, merge, or shared services).");
  }
  if (record.validation_passed) return;
  if (record.validation_log) throw new Error(`Previous integration validation was interrupted or failed. Inspect ${record.validation_log} and explicitly approve a retry before resuming.`);
  const root = join(config.state_dir, "omp-runs", record.run_id);
  record.validation_attempts = (record.validation_attempts ?? 0) + 1;
  const log = join(root, `validation-${record.validation_attempts}.log`);
  const fd = openSync(log, "wx", 0o600);
  record.validation_log = log; save(record, config);
  try {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const child = spawn("/bin/sh", ["-c", command], { cwd: record.integration_dir, env: ensureWorkerEnv(), stdio: ["ignore", fd, fd] });
    child.on("error", reject);
    child.on("close", code => code === 0 ? resolve() : reject(new Error(`Integration validation exited ${code}; see ${record.validation_log}`)));
    await promise;
  } finally { closeSync(fd); }
  record.validation_passed = true; save(record, config);
}
async function supervise(config: Config, record: RunRecord): Promise<RunRecord> {
  let heartbeat: NodeJS.Timeout | undefined;
  let heartbeatError: unknown;
  let renewing = false;
  let lastSessionRenewal = 0;
  let assignmentReady = false;
  const renew = async (sessions: boolean): Promise<void> => {
    if (renewing) return;
    renewing = true;
    try {
      if (sessions) {
        for (const credential of [config.supervisor_credential_file, record.coder_credential_file, ...record.reviewer_credential_files]) {
          await call(config, credential, "sessions.renew", {}, randomUUID());
        }
        lastSessionRenewal = Date.now();
      }
      if (assignmentReady) {
        await call(config, record.coder_credential_file, "assignments.lease.renew", { task_id: record.task_id }, randomUUID());
      }
    } finally { renewing = false; }
  };
  try {
    await renew(true);
    heartbeat = setInterval(() => {
      if (heartbeatError) return;
      void renew(Date.now() - lastSessionRenewal >= 10 * 60_000).catch(error => { heartbeatError = error; });
    }, 40_000);
    while (record.phase !== "done") {
      if (heartbeatError) throw new Error(`Credential or assignment lease renewal failed: ${cleanError(heartbeatError)}`);
      const task = (await listed<TaskState>(config, config.supervisor_credential_file, "tasks.list", { run_id: record.run_id })).find(t => t.id === record.task_id);
      if (!task) throw new Error("Run's task no longer exists; cannot recover automatically.");
      if (task.status === "completed" && record.phase !== "integrating") record.phase = "integrating";
      if (task.status === "submitted" && (record.phase === "coding" || record.phase === "revision")) { record.phase = "review"; save(record, config); }
      if (task.status === "revision_required" && record.phase === "review") {
        record.previous_finding_ids = [...new Set((await listed<ReviewState>(config, config.supervisor_credential_file, "reviews.list", { task_id: record.task_id })).flatMap(r => r.findings.map(f => f.id)))];
        record.round++; record.phase = "revision"; record.commit_refs = undefined; record.round_id = undefined; save(record, config);
      }
      if (record.phase === "coding" || record.phase === "revision") {
        const revision = record.phase === "revision";
        if (task.status === "revision_required") {
          await mutate(config, record, "tasks.assign", { task_id: record.task_id, assignee: record.coder_actor_id }, `assign:${record.round}`);
        } else if (task.status !== "assigned" && task.status !== "coding") {
          throw new Error(`Unexpected coder task status ${task.status}; inspect run before continuing.`);
        }
        if (task.status !== "coding") await mutate(config, record, "tasks.claim", { task_id: record.task_id }, `claim:${record.round}`, record.coder_credential_file);
        await call(config, record.coder_credential_file, "assignments.lease.renew", { task_id: record.task_id }, randomUUID());
        assignmentReady = true;
        if (revision && !record.workers[`coder-round-${record.round}`]) {
          const workspace = await mutate<WorkspaceState>(config, record, "workspaces.request", { task_id: record.task_id, repository_id: record.repository_id, base_commit: record.last_head }, `workspace:${record.round}`);
          record.workspace_id = workspace.id; record.workspace_dir = workspace.path; save(record, config);
        }
        const priorFindings = revision ? (await listed<ReviewState>(config, config.supervisor_credential_file, "reviews.list", { task_id: record.task_id }))
          .flatMap(r => r.findings).filter(f => record.previous_finding_ids.includes(f.id))
          .map(f => `${f.location}: ${f.evidence} — ${f.proposed_correction}`).join("\n") : "";
        const prompt = `Implement Tesselate task ${record.task_id}, round ${record.round}. Goal: ${record.spec.goal}\nAcceptance criteria: ${record.spec.acceptance_criteria}\nConstraints: ${record.spec.constraints}\n${revision ? `Address these reviewer findings with new commits:\n${priorFindings}` : ""}\nRead repository AGENTS.md before coding. This is the assigned isolated task worktree, not integration/main. Do not push, merge, run tests or start shared app/server/Redis services in this task worktree. Bitwars .env and packages/content/.revisions do not travel with worktrees: do not copy secrets or assume they exist. Commit your changes on this task branch. Do not use any supervisor credential or Tesselate operations. End with a concise plain-text summary; the supervisor inspects and captures committed refs.`;
        await launch(record, config, `coder-round-${record.round}`, "coder", record.workspace_dir, record.coder_credential_file, prompt);
        if (heartbeatError) throw new Error(`Credential or assignment lease renewal failed: ${cleanError(heartbeatError)}`);
        if (!record.commit_refs) await capture(config, record);
        assignmentReady = false;
        if (task.status === "coding" || task.status === "assigned" || task.status === "revision_required") {
          await mutate(config, record, "tasks.submit", { task_id: record.task_id, commits: record.commit_refs, summary: `Committed work round ${record.round}: ${record.last_head}` }, `submit:${record.round}`, record.coder_credential_file);
        }
        record.phase = "review"; save(record, config);
      } else if (record.phase === "review") {
        if (!record.commit_refs?.length) throw new Error("Missing captured commit refs; cannot review.");
        if (task.status === "submitted") {
          const round = await mutate<RoundState>(config, record, "reviews.request", { task_id: record.task_id, commit_refs: record.commit_refs, previous_finding_ids: record.previous_finding_ids }, `round:${record.round}`);
          record.round_id = round.id; save(record, config);
        } else if (task.status !== "in_review") throw new Error(`Unexpected review task status ${task.status}.`);
        const existing = await listed<RoundState>(config, config.supervisor_credential_file, "reviews.rounds.list", { task_id: record.task_id });
        record.round_id ||= existing.find(r => r.round === record.round)?.id;
        if (!record.round_id) throw new Error("Review round missing after restart.");
        const reviews = await performReviews(config, record);
        if (heartbeatError) throw new Error(`Credential or assignment lease renewal failed: ${cleanError(heartbeatError)}`);
        const findings = (await listed<ReviewState>(config, config.supervisor_credential_file, "reviews.list", { task_id: record.task_id }))
          .filter(r => r.round_id === record.round_id).flatMap(r => r.findings);
        if (findings.length) {
          const ids = findings.map(f => f.id as string);
          await mutate(config, record, "reviews.disposition", { task_id: record.task_id,
            dispositions: ids.map(id => ({ finding_id: id, disposition: "fixed", reason: "Requires a new commit and independent review round." })) }, `disposition:${record.round}`);
          record.previous_finding_ids.push(...ids);
          record.round++; record.phase = "revision"; record.commit_refs = undefined; record.round_id = undefined;
        } else {
          if (reviews.length !== 2 || reviews.some(r => !r.passed)) throw new Error("Two independent passes are required before integration.");
          await mutate(config, record, "tasks.disposition", { task_id: record.task_id, outcome: "completed", reason: "Two distinct non-author reviewers passed the captured commits." }, `complete-task:${record.round}`);
          record.phase = "integrating";
        }
        save(record, config);
      } else if (record.phase === "integrating") {
        if (await git(record.integration_dir, "rev-parse", "--abbrev-ref", "HEAD") !== record.branch) throw new Error("Integration branch changed; refuse merge.");
        const head = await git(record.integration_dir, "rev-parse", "HEAD");
        if (head === record.base) {
          if (await git(record.integration_dir, "status", "--porcelain")) throw new Error("Integration checkout dirty; refuse merge.");
          await git(record.integration_dir, "merge", "--ff-only", record.last_head!);
        } else if (head !== record.last_head) throw new Error("Integration branch changed unexpectedly; refuse merge.");
        await validate(record, config);
        if (await git(record.integration_dir, "rev-parse", "--abbrev-ref", "HEAD") !== record.branch ||
            await git(record.integration_dir, "rev-parse", "HEAD") !== record.last_head ||
            await git(record.integration_dir, "status", "--porcelain")) {
          throw new Error("Integration validation changed the branch, HEAD, or tracked/untracked files; inspect before completing.");
        }
        await mutate(config, record, "runs.complete", { run_id: record.run_id, outstanding_integration_steps: [] }, "complete-run");
        await cli(config, config.supervisor_credential_file, ["artifact", "export", "--run-id", record.run_id]);
        record.phase = "done"; record.error = undefined; save(record, config);
      }
    }
    if (heartbeatError) throw new Error(`Credential or assignment lease renewal failed: ${cleanError(heartbeatError)}`);
  } catch (error) {
    record.error = cleanError(error); save(record, config);
    throw error;
  } finally {
    clearInterval(heartbeat);
  }
  return record;
}
async function createRun(config: Config, repo: string, requested: Spec): Promise<RunRecord> {
  const ensured = await cli<{ project_id: string; repository_id: string; canonical_path: string }>(config, config.supervisor_credential_file, ["project", "ensure", "--repo", repo]);
  const { project_id, repository_id } = ensured;
  if (!project_id || !repository_id || ensured.canonical_path !== repo) throw new Error("Project ensure returned a different repository or no registration.");
  const previous = readIntent(config, repo);
  const runs = await listed<RunSummary>(config, config.supervisor_credential_file, "runs.list", { project_id });
  if (!previous && runs.some(r => r.status === "active" && !load(config, r.id))) {
    throw new Error("An existing Tesselate run has no OMP handoff; inspect it before starting another run.");
  }
  const intent: RunIntent = previous ?? { version: 1, repository: repo, project_id, repository_id, spec: requested,
    base: await git(repo, "rev-parse", "--verify", `${requested.base_branch}^{commit}`), key: `omp:new:${randomUUID()}`,
    supervisor_credential_file: config.supervisor_credential_file };
  if (intent.project_id !== project_id || intent.repository_id !== repository_id) throw new Error("Pending run intent references another project; inspect before continuing.");
  if (!previous) saveIntent(config, intent, true); // Durable idempotency identity before runs.create.
  const spec = intent.spec;
  const existing = intent.run_id ? runs.find(r => r.id === intent.run_id) : undefined;
  if (intent.run_id && !existing) throw new Error("Pending run ID is missing from Tesselate; inspect its state before continuing.");
  if (!intent.run_id && intent.supervisor_credential_file !== config.supervisor_credential_file) {
    throw new Error("Supervisor session changed before the run ID was recorded; reconcile the pending run manually to avoid duplicate creation.");
  }
  const run = existing ?? value(await call<{ value: RunSummary }>(config, config.supervisor_credential_file, "runs.create",
    { project_id, goal: `${spec.goal}\nAcceptance: ${spec.acceptance_criteria}\nConstraints: ${spec.constraints}` }, intent.key));
  if (intent.run_id && intent.run_id !== run.id) throw new Error("Run creation replay returned a different run.");
  intent.run_id = run.id;
  saveIntent(config, intent);
  const branch = `tesselate/${run.id}`;
  const integration_dir = join(config.state_dir, "omp-runs", run.id, "integration");
  const record: RunRecord = load(config, run.id) ?? { version: 1, project_id, repository_id, repository: repo,
    run_id: run.id, task_id: "", base: intent.base, base_branch: spec.base_branch, branch, integration_dir,
    workspace_id: "", workspace_dir: "", coder_credential_file: "", coder_actor_id: "", reviewer_credential_files: [],
    reviewer_actor_ids: [], spec, phase: "coding", round: 1, previous_finding_ids: [], workers: {} };
  if (record.repository !== repo || record.base !== intent.base || record.branch !== branch) throw new Error("Run handoff disagrees with its durable intent.");
  save(record, config);
  if (record.workspace_id) {
    unlinkSync(intentPath(config, repo));
    return record;
  }
  if (!existsSync(integration_dir)) {
    const existingBranch = await git(repo, "show-ref", "--verify", `refs/heads/${branch}`).catch(() => "");
    if (existingBranch) {
      if (await git(repo, "rev-parse", branch) !== intent.base) throw new Error("Existing integration branch moved; preserve for inspection.");
      await git(repo, "worktree", "add", integration_dir, branch);
    } else {
      await git(repo, "worktree", "add", "-b", branch, integration_dir, intent.base);
    }
  }
  if (await git(integration_dir, "rev-parse", "--abbrev-ref", "HEAD") !== branch ||
      await git(integration_dir, "rev-parse", "HEAD") !== intent.base) throw new Error("Integration worktree does not match the durable base intent.");
  if (!record.task_id) {
    const task = await mutate<TaskState>(config, record, "tasks.create",
      { run_id: record.run_id, title: spec.goal.slice(0, 100), goal: `${spec.goal}\nAcceptance: ${spec.acceptance_criteria}\nConstraints: ${spec.constraints}` }, "task");
    record.task_id = task.id; save(record, config);
  }
  if (!record.coder_credential_file) {
    const coder = await cli<CredentialState>(config, config.supervisor_credential_file,
      ["credential", "issue", "--actor-name", `omp-coder-${run.id}`, "--role", "coder", "--idempotency-key", `omp:${run.id}:coder`]);
    record.coder_credential_file = credentialPath(config, coder);
    record.coder_actor_id = coder.actor_id; save(record, config);
  }
  for (let i = record.reviewer_credential_files.length; i < 2; i++) {
    const reviewer = await cli<CredentialState>(config, config.supervisor_credential_file,
      ["credential", "issue", "--actor-name", `omp-reviewer-${i}-${run.id}`, "--role", "reviewer", "--idempotency-key", `omp:${run.id}:reviewer:${i}`]);
    record.reviewer_credential_files.push(credentialPath(config, reviewer));
    record.reviewer_actor_ids.push(reviewer.actor_id);
    save(record, config);
  }
  await mutate(config, record, "tasks.assign", { task_id: record.task_id, assignee: record.coder_actor_id }, "assign:1");
  await call(config, record.coder_credential_file, "assignments.lease.renew", { task_id: record.task_id }, randomUUID());
  const workspace = await mutate<WorkspaceState>(config, record, "workspaces.request",
    { task_id: record.task_id, repository_id, base_commit: record.base }, "workspace");
  record.workspace_id = workspace.id; record.workspace_dir = workspace.path; record.last_head = record.base;
  save(record, config);
  unlinkSync(intentPath(config, repo));
  return record;
}

// Exported for a deterministic, no-model smoke fixture. Production uses the same path.
export async function startFeature(config: Config, cwd: string, spec: Spec, ui: UIOps): Promise<RunRecord> {
  const repo = await repoFor(cwd, config);
  const pendingIntent = readIntent(config, repo);
  if (pendingIntent) {
    if (!await ui.confirm(`Resume interrupted Tesselate run setup for ${pendingIntent.spec.goal}?`)) throw new Error("Pending run setup left untouched.");
    const record = await createRun(config, repo, pendingIntent.spec);
    return record.phase === "done" ? record : supervise(config, record);
  }
  if (!spec.goal.trim() || !spec.acceptance_criteria.trim()) throw new Error("Goal and acceptance criteria are required.");
  if (!spec.validation_command.trim()) throw new Error("Choose an explicit integration-only validation command before starting.");
  const ensured = await cli<{ project_id: string; repository_id: string; canonical_path: string }>(config, config.supervisor_credential_file, ["project", "ensure", "--repo", repo]);
  const existingRun = (await listed<RunSummary>(config, config.supervisor_credential_file, "runs.list", { project_id: ensured.project_id }))
    .filter(r => r.status === "active" || r.status === "completed")
    .map(r => ({ run: r, record: load(config, r.id) })).filter(item => item.record?.repository === repo);
  if (existingRun.length) {
    const selected = await ui.select("Resume or inspect an existing feature, or start another run?", [...existingRun.map(x => `Resume ${x.run.id}: ${x.run.goal.slice(0, 60)}`), "Start new run"]);
    if (!selected) throw new Error("No run selected.");
    const match = existingRun.find(x => selected.startsWith(`Resume ${x.run.id}:`));
    if (match) {
      const record = match.record!;
      if (record.phase === "done") return record;
      if (record.phase === "integrating" && record.validation_log && !record.validation_passed) {
        if (!await ui.confirm(`Prior integration validation may have failed or been interrupted. Inspect ${record.validation_log}; retry the same approved command on ${record.integration_dir}?`)) {
          throw new Error(`Validation not retried; inspect ${record.validation_log}.`);
        }
        record.validation_log = undefined; save(record, config);
      }
      return supervise(config, record);
    }
  }
  const base = await selectBase(repo, ui);
  spec.base_branch = base.branch;
  if (repo.endsWith("/bitwars") && !await ui.confirm("Bitwars requires integration-only provisioning of ignored .env and packages/content/.revisions. No secrets will be copied automatically. Continue?")) {
    throw new Error("Bitwars prerequisites not approved.");
  }
  if (!await ui.confirm(`Start Tesselate feature on ${base.branch}@${base.base.slice(0, 12)}? No push or main merge will occur.`)) throw new Error("Feature start cancelled.");
  if (await git(repo, "rev-parse", "--verify", `${base.branch}^{commit}`) !== base.base) throw new Error("Selected base moved; restart feature setup.");
  const record = await createRun(config, repo, spec);
  if (repo.endsWith("/bitwars")) ui.notify(`Integration checkout: ${record.integration_dir}. Provision ignored .env and packages/content/.revisions manually there if approved validation needs them; no automatic copy.`, "warning");
  return supervise(config, record);
}

export default function tesselateExtension(pi: ExtensionAPI): void {
  if (process.env.TESSELATE_WORKER === "1") return;
  const configPath = join(dirname(new URL(import.meta.url).pathname), "tesselate.config.json");
  let pending: { goal: string; acceptance: string; constraints: string; cwd: string; source_cwd: string } | undefined;
  function configured(): Config { return configAt(configPath); }
  async function kickoff(goal: string, cwd: string, ui: UIOps): Promise<void> {
    try {
      const source_cwd = cwd;
      cwd = await chooseRepo(cwd, configured(), ui);
      const acceptance = await ui.input("Acceptance criteria (observable result; required)");
      if (!acceptance?.trim()) return ui.notify("Feature cancelled: acceptance criteria are required.", "warning");
      const constraints = await ui.input("Constraints / prohibited actions (blank if none)");
      if (constraints === undefined) return;
      pending = { goal, acceptance, constraints, cwd, source_cwd };
      pi.sendUserMessage(`Draft a clear, concise feature specification from this user request. Goal: ${goal}\nAcceptance criteria: ${acceptance}\nConstraints: ${constraints || "None specified"}\nAsk clarifying questions if needed. Inspect repository test instructions and propose a safe integration-only validation_command. Then call tesselate_feature_start with the agreed goal, acceptance_criteria, constraints, and proposed validation_command. If the tool asks for a missing command, inspect the repository and retry. Do not start coding in this main session; the tool will confirm branch and launch separate OMP sessions.`, { attribution: "agent" });
    } catch (error) { ui.notify(cleanError(error), "error"); }
  }
  pi.on("input", async (event, ctx) => {
    if (event.source === "extension" || !event.text || event.text.startsWith("/") || event.text.startsWith("!") || event.text.startsWith("$")) return;
    if (!/\bfeature\b/i.test(event.text) && !/^\s*(?:please\s+)?(?:implement|build|add|create|fix|refactor)\b[\s\S]*\b(?:in|for)\s+(?:(?:this|the|a)\s+)?(?:repo|repository|project)\b/i.test(event.text)) return;
    await kickoff(event.text.trim(), ctx.cwd, ctx.ui);
    return { handled: true };
  });
  pi.registerCommand("feature", { description: "Start or resume a Tesselate feature", handler: async (args, ctx) => {
    const goal = args.trim() || await ctx.ui.input("What feature should Tesselate build?");
    if (goal?.trim()) await kickoff(goal.trim(), ctx.cwd, ctx.ui);
  } });
  pi.registerTool({ name: "tesselate_feature_start", label: "Start Tesselate feature", description: "After agreeing on a specification, launch role-separated OMP processes in Tesselate workspaces. Requires a user-initiated /feature or feature input.",
    parameters: pi.zod.object({ goal: pi.zod.string(), acceptance_criteria: pi.zod.string(), constraints: pi.zod.string(), validation_command: pi.zod.string() }),
    async execute(_id, params, _signal, _update, ctx) {
      if (!pending || pending.source_cwd !== ctx.cwd) return { content: [{ type: "text", text: "Use /feature or submit a feature request first; no workflow started." }] };
      const request = pending;
      if (!params.goal.trim() || !params.acceptance_criteria.trim()) return { content: [{ type: "text", text: "Goal and acceptance criteria are required." }] };
      const proposed = params.validation_command.trim();
      const entered = await ctx.ui.input(`Integration-only validation command (press Enter to use agent proposal: ${proposed || "none"})`);
      if (entered === undefined) return { content: [{ type: "text", text: "Validation selection cancelled; no run started." }] };
      const validation = entered.trim() || proposed;
      if (!validation) return { content: [{ type: "text", text: "No validation command proposed. Inspect repository test instructions, choose a safe integration-only validation command, and call tesselate_feature_start again; no run started." }] };
      if (!await ctx.ui.confirm(`Start independent Tesselate coder and reviewer sessions for: ${params.goal}? Validation in the integration worktree: ${validation}`)) return { content: [{ type: "text", text: "Cancelled; no run started." }] };
      pending = undefined;
      try {
        const result = await startFeature(configured(), request.cwd, { goal: params.goal, acceptance_criteria: params.acceptance_criteria, constraints: params.constraints, validation_command: validation, base_branch: "" }, ctx.ui);
        return { content: [{ type: "text", text: `Tesselate run ${result.run_id}: ${result.phase}. Integration branch ${result.branch}, ${result.integration_dir}. No push or main merge.` }] };
      } catch (error) {
        return { content: [{ type: "text", text: `Tesselate paused: ${cleanError(error)}. Use /feature to resume after resolving; no push or main merge was attempted.` }], isError: true };
      }
    }
  });
}
