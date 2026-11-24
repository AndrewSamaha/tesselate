import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

// A fake CLI and fake OMP process, but real Git worktrees/commits. Never calls a model.
const fakeCli = String.raw`#!/usr/bin/env node
const fs = require('node:fs'), cp = require('node:child_process'), path = require('node:path'), crypto = require('node:crypto');
const a = process.argv.slice(2), opt = key => a[a.indexOf(key)+1];
const stateDir = opt('--state-dir'), cred = opt('--credential-file');
const dbPath = path.join(stateDir, 'fixture-db.json');
const db = fs.existsSync(dbPath) ? JSON.parse(fs.readFileSync(dbPath,'utf8')) : { reviews: [], rounds: [], credentials: {}, workspaces: {} };
const id = () => crypto.randomUUID();
const git = (repo, ...args) => cp.execFileSync('git',['-C',repo,...args],{encoding:'utf8'}).trim();
const reply = value => { fs.writeFileSync(dbPath,JSON.stringify(db)); process.stdout.write(JSON.stringify(value)+'\n'); };
const idx = a.indexOf('call'), command = idx >= 0 ? a[idx+1].replace('tesselate.v1.','') : '';
const params = idx >= 0 ? JSON.parse(opt('--params')) : {};
const actor = db.credentials[cred]?.actor;
if (a.includes('project') && a.includes('ensure')) reply({project_id:'00000000-0000-4000-8000-000000000001',repository_id:'00000000-0000-4000-8000-000000000002',canonical_path:opt('--repo')});
else if (a.includes('credential') && a.includes('issue')) {
  const key=opt('--idempotency-key');
  db.issues??={};
  if(db.issues[key]) {
    const issued=db.issues[key];
    reply({actor_id:issued.actor_id,session_id:issued.session_id,credential_file:null,credential:null,replayed:true});
  } else {
    const actor_id=id(),session_id=id(),credential_file=path.join(stateDir,'credentials',session_id+'.token');
    fs.mkdirSync(path.dirname(credential_file),{recursive:true});fs.writeFileSync(credential_file,'role-only',{mode:0o600});
    db.credentials[credential_file]={actor:actor_id,role:opt('--role')};
    db.issues[key]={actor_id,session_id};
    fs.writeFileSync(dbPath,JSON.stringify(db));
    if(process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE==='1' && opt('--role')==='coder') process.exit(79);
    reply({actor_id,credential_file,session_id,credential:'intentionally-never-logged',replayed:false});
  }
}
else if (a.includes('artifact') && a.includes('export')) reply({run_id:opt('--run-id'),references:[]});
else if (command === 'runs.list') reply({items:db.run ? [db.run] : [],next_cursor:null});
else if (command === 'runs.create') {
  const key=opt('--idempotency-key');
  if(db.run) {
    if(db.runKey !== key) throw Error('attempted duplicate run');
    reply({value:db.run});
  } else {
    db.run={id:id(),project_id:params.project_id,goal:params.goal,status:'active'};
    db.runKey=key;db.runCreateCount=1;
    fs.writeFileSync(dbPath,JSON.stringify(db)); // Simulated crash after durable coordinator commit.
    if(process.env.FIXTURE_FAIL_AFTER_RUN_CREATE==='1') process.exit(79);
    reply({value:db.run});
  }
}
else if (command === 'tasks.create') { db.task={id:id(),run_id:params.run_id,status:'ready'};reply({value:db.task}); }
else if (command === 'tasks.list') reply({items:db.task ? [db.task] : [],next_cursor:null});
else if (command === 'tasks.assign') {
  if (!['ready','revision_required'].includes(db.task.status)) throw Error('assignment requires ready/revision_required task');
  db.task.status='assigned'; db.task.owner=params.assignee;reply({value:db.task});
}
else if (command === 'tasks.claim') {db.task.status='coding';reply({value:db.task});}
else if (command === 'sessions.renew') reply({value:new Date(Date.now()+12*3600000).toISOString()});
else if (command === 'assignments.lease.renew') reply({value:{assignment_id:id(),task_id:params.task_id,expires_at:new Date(Date.now()+120000).toISOString()}});
else if (command === 'workspaces.request') {
  const ws={id:id(),path:path.join(stateDir,'workspaces',id()),base_commit:params.base_commit,task_id:params.task_id,repository_id:params.repository_id};
  fs.mkdirSync(path.dirname(ws.path),{recursive:true});git(process.env.FIXTURE_REPO,'worktree','add','-b','task-'+ws.id,ws.path,ws.base_commit);
  db.workspaces[ws.id]=ws;reply({value:ws});
}
else if (command === 'workspaces.inspect') {
  const ws=db.workspaces[params.workspace_id], head=git(ws.path,'rev-parse','HEAD');
  reply({workspace:ws,head:{repository_id:ws.repository_id,commit_id:head},clean:git(ws.path,'status','--porcelain')===''});
}
else if (command === 'workspaces.commits.capture') {
  const key=opt('--idempotency-key');
  db.captures??={};
  if (db.captures[key]) reply({value:db.captures[key]});
  else {
    const ws=db.workspaces[params.workspace_id];
    const refs=git(ws.path,'rev-list','--reverse',ws.base_commit+'..'+params.expected_head).split('\n').filter(Boolean)
      .map(commit_id=>({repository_id:ws.repository_id,commit_id}));
    db.captures[key]=refs;fs.writeFileSync(dbPath,JSON.stringify(db));
    if (process.env.FIXTURE_FAIL_AFTER_CAPTURE==='1') process.exit(79);
    reply({value:refs});
  }
}
else if (command === 'tasks.submit') {db.task.status='submitted';db.task.submitted_commits=params.commits;reply({value:db.task});}
else if (command === 'reviews.request') {
  db.task.status='in_review';const round={id:id(),round:db.rounds.length+1,task_id:params.task_id,commit_refs:params.commit_refs};
  db.rounds.push(round);reply({value:round});
}
else if (command === 'reviews.rounds.list') reply({items:db.rounds,next_cursor:null});
else if (command === 'reviews.list') reply({items:db.reviews,next_cursor:null});
else if (command === 'reviews.submit') {
  if(db.credentials[cred].role !== 'reviewer') throw Error('review used non-reviewer credential');
  const review={...params,id:id(),reviewer:actor};db.reviews.push(review);reply({value:review});
}
else if (command === 'reviews.disposition') { db.dispositions=params.dispositions;db.task.status='revision_required';reply({value:db.task}); }
else if (command === 'tasks.disposition') {db.task.status='completed';reply({value:db.task});}
else if (command === 'runs.complete') {db.run.status='completed';reply({value:db.run});}
else throw Error('unsupported fixture operation '+command+' '+a.join(' '));
`;
const fakeOmp = String.raw`#!/usr/bin/env node
const fs=require('node:fs'),cp=require('node:child_process'),path=require('node:path');
const a=process.argv.slice(2),cwd=a[a.indexOf('--cwd')+1],prompt=a[a.length-1];
const role=prompt.startsWith('Independent reviewer')?'reviewer':'coder';
fs.appendFileSync(path.join(process.env.FIXTURE_STATE,'worker-launches.jsonl'),JSON.stringify({role,cwd,readOnly:a.includes('--tools=read,grep,glob'),workerEnv:process.env.TESSELATE_WORKER})+'\n');
const round = Number(prompt.match(/round (\d+)/)?.[1] ?? 1);
if(role==='coder') {
  fs.writeFileSync(path.join(cwd,'feature.txt'),round===1?'implemented in task worktree\n':'feature.txt is committed (review correction)\n');
  cp.execFileSync('git',['-C',cwd,'add','feature.txt']);
  cp.execFileSync('git',['-C',cwd,'-c','user.name=Fixture Coder','-c','user.email=coder@example.invalid','commit','-m','Implement feature round '+round]);
  process.stdout.write('Committed implementation.\n');
} else if (round===1 && prompt.startsWith('Independent reviewer 1')) {
  process.stdout.write(JSON.stringify({passed:false,summary:'Missing explicit acceptance text',findings:[{severity:'medium',location:'feature.txt:1',evidence:'Expected acceptance phrase absent',proposed_correction:'Include acceptance phrase'}]})+'\n');
} else process.stdout.write(JSON.stringify({passed:true,summary:'Reviewed committed change independently',findings:[]})+'\n');
`;

function git(repo: string, ...args: string[]): string {
  return execFileSync("git", ["-C", repo, ...args], { encoding: "utf8" }).trim();
}

export async function runSmoke(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "tesselate-omp-fixture-"));
  const previousPath = process.env.PATH;
  const previousRepo = process.env.FIXTURE_REPO;
  const previousState = process.env.FIXTURE_STATE;
  const previousCrash = process.env.FIXTURE_FAIL_AFTER_RUN_CREATE;
  const previousIssueCrash = process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE;
  const previousCaptureCrash = process.env.FIXTURE_FAIL_AFTER_CAPTURE;
  try {
    const home = join(root, "home"), workspace_root = join(home, "Development"), repo = join(workspace_root, "sample"), state_dir = join(workspace_root, ".tesselate");
    const ext = join(home, ".omp", "agent", "extensions");
    mkdirSync(repo, { recursive: true }); mkdirSync(ext, { recursive: true }); mkdirSync(state_dir, { recursive: true });
    execFileSync("git", ["init", "-q", "-b", "main", repo]);
    writeFileSync(join(repo, "README.md"), "starting point\n");
    git(repo, "add", "README.md");
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "Base");
    const mainHead = git(repo, "rev-parse", "main");
    const bin = join(root, "bin"); mkdirSync(bin);
    const binary = join(bin, "tesselate");
    writeFileSync(binary, fakeCli, { mode: 0o755 });
    writeFileSync(join(bin, "omp"), fakeOmp, { mode: 0o755 });
    process.env.PATH = bin + ":" + previousPath;
    process.env.FIXTURE_REPO = repo; process.env.FIXTURE_STATE = state_dir;
    const supervisor_credential_file = join(state_dir, "supervisor.token");
    writeFileSync(supervisor_credential_file, "supervisor-token", { mode: 0o600 });
    copyFileSync(join(import.meta.dir, "tesselate.ts"), join(ext, "tesselate.ts"));
    writeFileSync(join(ext, "tesselate.config.json"), JSON.stringify({ binary, state_dir, workspace_root, supervisor_credential_file }), { mode: 0o600 });
    const extension = (await import(pathToFileURL(join(ext, "tesselate.ts")).href)).default;
    let inputHandler: (event: { text: string; source: string }, ctx: unknown) => Promise<unknown> = async () => undefined;
    let tool: { execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> } | undefined;
    const messages: string[] = [];
    const ui = { input: async (question: string) => question.startsWith("Acceptance") ? "feature.txt is committed" : "no shared-service tests",
      select: async (question: string, choices: string[]) => question.startsWith("Which repository") ? "sample" : question.startsWith("Resume") ? choices[0] : "main",
      confirm: async () => true, notify: () => {} };
    const pi = { zod: { object: () => ({}), string: () => ({}) }, on: (event: string, handler: typeof inputHandler) => { if (event === "input") inputHandler = handler; },
      registerCommand: () => {}, registerTool: (definition: typeof tool) => { tool = definition; },
      sendUserMessage: (message: string) => { messages.push(message); } };
    // The test intentionally imports the installed extension by its runtime-selected user path.
    extension(pi as unknown as Parameters<typeof extension>[0]);
    assert.equal(await inputHandler({ source: "interactive", text: "fix the bug" }, { cwd: workspace_root, ui }), undefined);
    assert.equal(messages.length, 0);
    const result = await inputHandler({ source: "interactive", text: "Build a feature for this repo that writes feature.txt" }, { cwd: workspace_root, ui });
    assert.deepEqual(result, { handled: true });
    assert.match(messages[0], /feature\.txt is committed/);
    assert.match(messages[0], /no shared-service tests/);
    assert.ok(tool);
    process.env.FIXTURE_FAIL_AFTER_RUN_CREATE = "1";
    const interrupted = await tool!.execute("call", { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "no shared-service tests", validation_command: "git diff --exit-code HEAD -- README.md" }, undefined, undefined, { cwd: workspace_root, ui: {
      ...ui, input: async () => "git diff --exit-code HEAD -- README.md"
    } });
    assert.match(interrupted.content[0].text, /paused/);
    assert.equal(readdirSync(join(state_dir, "omp-runs", "intents")).length, 1);
    delete process.env.FIXTURE_FAIL_AFTER_RUN_CREATE;
    // A second interruption after role-token persistence replays issuance without the secret.
    const { startFeature } = await import(pathToFileURL(join(ext, "tesselate.ts")).href);
    const config = { binary, state_dir, workspace_root, supervisor_credential_file };
    const spec = { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "", validation_command: "git diff --exit-code HEAD -- README.md", base_branch: "" };
    process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE = "1";
    await assert.rejects(startFeature(config, repo, spec, ui));
    delete process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE;
    process.env.FIXTURE_FAIL_AFTER_CAPTURE = "1";
    await assert.rejects(startFeature(config, repo, spec, ui));
    assert.equal(readFileSync(join(state_dir, "worker-launches.jsonl"), "utf8").trim().split("\n").length, 1);
    delete process.env.FIXTURE_FAIL_AFTER_CAPTURE;
    const completed = await startFeature(config, repo, spec, ui);
    assert.equal(completed.phase, "done");
    const record = JSON.parse(readFileSync(join(state_dir, "omp-runs", completed.run_id, "record.json"), "utf8"));
    assert.equal(record.phase, "done");
    assert.equal(git(repo, "rev-parse", "main"), mainHead);
    assert.equal(git(record.integration_dir, "rev-parse", "HEAD"), record.last_head);
    const launches = readFileSync(join(state_dir, "worker-launches.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    assert.deepEqual(launches.map(item => item.role), ["coder", "reviewer", "reviewer", "coder", "reviewer", "reviewer"]);
    assert.notEqual(launches[0].cwd, launches[3].cwd);
    assert.equal(launches[3].cwd, record.workspace_dir);
    assert.ok(launches.filter(item => item.role === "reviewer").every(item => item.readOnly && item.cwd !== record.workspace_dir && item.workerEnv === "1"));
    const database = JSON.parse(readFileSync(join(state_dir, "fixture-db.json"), "utf8"));
    assert.equal(database.rounds.length, 2);
    assert.equal(database.dispositions[0].disposition, "fixed");
    assert.equal(database.task.submitted_commits[0].commit_id, record.last_head);
    assert.equal(new Set(database.reviews.map((review: { reviewer: string }) => review.reviewer)).size, 2);
    // Reopening a completed run reads the durable handoff; no worker starts again.
    const resumed = await startFeature({ binary, state_dir, workspace_root, supervisor_credential_file }, repo,
      { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "", validation_command: "git diff --exit-code HEAD -- README.md", base_branch: "" }, ui);
    assert.equal(resumed.run_id, record.run_id);
    assert.equal(readFileSync(join(state_dir, "worker-launches.jsonl"), "utf8").trim().split("\n").length, 6);
    assert.equal(database.runCreateCount, 1);
  } finally {
    if (previousPath === undefined) delete process.env.PATH; else process.env.PATH = previousPath;
    if (previousRepo === undefined) delete process.env.FIXTURE_REPO; else process.env.FIXTURE_REPO = previousRepo;
    if (previousState === undefined) delete process.env.FIXTURE_STATE; else process.env.FIXTURE_STATE = previousState;
    if (previousCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_RUN_CREATE; else process.env.FIXTURE_FAIL_AFTER_RUN_CREATE = previousCrash;
    if (previousIssueCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE; else process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE = previousIssueCrash;
    if (previousCaptureCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_CAPTURE; else process.env.FIXTURE_FAIL_AFTER_CAPTURE = previousCaptureCrash;
    rmSync(root, { recursive: true, force: true });
  }
}
