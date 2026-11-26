import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
const here = dirname(fileURLToPath(import.meta.url));

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
    db.run={id:id(),project_id:params.project_id,goal:params.goal,status:'active',
      limits:{max_task_retries:2,max_review_rounds:process.env.FIXTURE_LOW_REVIEW_BUDGET==='1'?1:3}};
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
  reply({workspace:ws,head:{repository_id:ws.repository_id,commit_id:head},clean:git(ws.path,'status','--porcelain','--ignored=traditional')==='',capture_ready:git(ws.path,'status','--porcelain')===''});
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
else if (command === 'tasks.repair') {
  db.repairs=(db.repairs??0)+1;
  db.task.status='revision_required';
  db.run.limits={max_task_retries:Math.max(db.run.limits.max_task_retries,db.task.attempts??0),max_review_rounds:Math.max(db.run.limits.max_review_rounds,2)};
  reply({value:{run:db.run,task:db.task}});
}
else if (command === 'tasks.disposition') {db.task.status='completed';reply({value:db.task});}
else if (command === 'runs.complete') {db.run.status='completed';reply({value:db.run});}
else throw Error('unsupported fixture operation '+command+' '+a.join(' '));
`;
const fakeOmp = String.raw`#!/usr/bin/env node
const fs=require('node:fs'),cp=require('node:child_process'),path=require('node:path');
const a=process.argv.slice(2),cwd=a[a.indexOf('--cwd')+1],prompt=a[a.length-1];
const role=prompt.startsWith('Independent planner')?'planner':prompt.startsWith('Select a Tesselate plan')?'plan selector':
  prompt.startsWith('Compare alternate')?'comparison':prompt.startsWith('Independent reviewer')?'reviewer':'coder';
const session=a[a.indexOf('--session-dir')+1];
const slot=path.basename(session).match(/^session-candidate-([ab])-/)?.[1];
const start=cp.execFileSync('git',['-C',cwd,'rev-parse','HEAD'],{encoding:'utf8'}).trim();
fs.appendFileSync(path.join(process.env.FIXTURE_STATE,'worker-launches.jsonl'),JSON.stringify({role,cwd,slot,start,readOnly:a.includes('--tools=read,grep,glob'),workerEnv:process.env.TESSELATE_WORKER,model:a[a.indexOf('--model')+1]})+'\n');
const round = Number(prompt.match(/round (\d+)/)?.[1] ?? 1);
if(role==='planner') {
  const proposal=prompt.match(/^Independent planner ([AB])/)[1];
  if(proposal==='B' && process.env.FIXTURE_BAD_PLANNER==='1') {
    process.stdout.write('Incomplete proposal.\n');
  } else {
    process.stdout.write(JSON.stringify({approach:'Proposal '+proposal,steps:['Write feature.txt'],verification:['Assert feature.txt is committed'],risks:[]})+'\n');
  }
} else if(role==='plan selector') {
  if(!prompt.includes('Proposal A')||!prompt.includes('Proposal B')) throw Error('Plan selection needs both proposals.');
  process.stdout.write(JSON.stringify({recommendation:'B',reason:'Plan B directly verifies the acceptance criterion.'})+'\n');
} else if(role==='coder') {
  if(!prompt.includes('Supervisor-selected implementation plan: {"approach":"Proposal B"')||prompt.includes('Proposal A')) {
    throw Error('Expected only the selected proposal in the coder brief.');
  }
  fs.writeFileSync(path.join(cwd,'feature.txt'),round===1?'implemented in task worktree\n':'feature.txt is committed (review correction)\n');
  if(slot) fs.appendFileSync(path.join(cwd,'feature.txt'),'Candidate '+slot+'\n');
  if(slot==='a' && process.env.FIXTURE_REJECT_CANDIDATE_A==='1') {
    fs.appendFileSync(path.join(cwd,'README.md'),'Out-of-scope candidate change\n');
    cp.execFileSync('git',['-C',cwd,'add','README.md']);
  }
  cp.execFileSync('git',['-C',cwd,'add','feature.txt']);
  if(process.env.FIXTURE_TRACK_IGNORED==='1') {
    fs.writeFileSync(path.join(cwd,'.env'),'worker-dummy-not-the-source-secret\n');
    cp.execFileSync('git',['-C',cwd,'add','-f','.env']);
  }
  cp.execFileSync('git',['-C',cwd,'-c','user.name=Fixture Coder','-c','user.email=coder@example.invalid','commit','-m','Implement feature round '+round]);
  fs.mkdirSync(path.join(cwd,'.workmux-local'),{recursive:true});
  fs.writeFileSync(path.join(cwd,'.workmux-local','completion.md'),'Coder completion handoff\n');
  process.stdout.write('Committed implementation.\n');
} else if(role==='comparison') {
  if(!prompt.includes('"candidate":"A"')||!prompt.includes('"candidate":"B"')) throw Error('Comparison needs both reviewed candidates.');
  process.stdout.write('Working...\n'+JSON.stringify({recommendation:'A',reason:'Fixture recommends A over B.'})+'\n');
} else {
  const marker = path.join(process.env.FIXTURE_STATE,'retry-review.marker');
  if(process.env.FIXTURE_RETRY_REVIEW==='1' && !fs.existsSync(marker)) {
    fs.writeFileSync(marker,'1');
    process.stdout.write('Working...\nCodex error event: usage limit reached (code=usage_limit_reached)\n');
    process.exit(1);
  }
  const review = slot!=='b' && round===1 && prompt.startsWith('Independent reviewer 1')
    ? {passed:false,summary:'Missing explicit acceptance text',findings:[{severity:'medium',location:'feature.txt:1',evidence:'Expected acceptance phrase absent',proposed_correction:'Include acceptance phrase'}]}
    : {passed:true,summary:'Reviewed committed change independently',findings:[]};
  process.stdout.write('Working...\n');
  process.stdout.write(JSON.stringify(review,null,round===1?2:undefined)+'\n');
  if(process.env.FIXTURE_TRAILING_REVIEW==='1') process.stdout.write('Unexpected trailing output\n');
}
`;

function git(repo: string, ...args: string[]): string {
  return execFileSync("git", ["-C", repo, ...args], { encoding: "utf8" }).trim();
}

export async function runSmoke(trackIgnored = false, trailingReview = false, retryReview = false, workerModel = "", repairReview = false): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "tesselate-omp-fixture-"));
  const previousPath = process.env.PATH;
  const previousRepo = process.env.FIXTURE_REPO;
  const previousState = process.env.FIXTURE_STATE;
  const previousCrash = process.env.FIXTURE_FAIL_AFTER_RUN_CREATE;
  const previousIssueCrash = process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE;
  const previousCaptureCrash = process.env.FIXTURE_FAIL_AFTER_CAPTURE;
  const previousTracked = process.env.FIXTURE_TRACK_IGNORED;
  const previousTrailing = process.env.FIXTURE_TRAILING_REVIEW;
  const previousRetry = process.env.FIXTURE_RETRY_REVIEW;
  const previousLowBudget = process.env.FIXTURE_LOW_REVIEW_BUDGET;
  const previousBadPlanner = process.env.FIXTURE_BAD_PLANNER;
  if (repairReview) process.env.FIXTURE_LOW_REVIEW_BUDGET = "1";
  try {
    const home = join(root, "home"), workspace_root = join(home, "Development"), repo = join(workspace_root, "sample"), state_dir = join(workspace_root, ".tesselate");
    const ext = join(home, ".omp", "agent", "extensions");
    mkdirSync(repo, { recursive: true }); mkdirSync(ext, { recursive: true }); mkdirSync(state_dir, { recursive: true });
    execFileSync("git", ["init", "-q", "-b", "main", repo]);
    writeFileSync(join(repo, "README.md"), "starting point\n");
    writeFileSync(join(repo, ".gitignore"), ".env*\n/packages/content/.revisions/\n/.workmux-local/\n");
    git(repo, "add", "README.md", ".gitignore");
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "Base");
    const mainHead = git(repo, "rev-parse", "main");
    writeFileSync(join(repo, ".env"), "fixture-secret=not-for-reviewers\n", { mode: 0o600 });
    mkdirSync(join(repo, "packages/content/.revisions"), { recursive: true });
    writeFileSync(join(repo, "packages/content/.revisions/snapshot.json"), "{\"id\":1}\n");
    const bin = join(root, "bin"); mkdirSync(bin);
    const binary = join(bin, "tesselate");
    if (trackIgnored) process.env.FIXTURE_TRACK_IGNORED = "1";
    writeFileSync(binary, fakeCli, { mode: 0o755 });
    writeFileSync(join(bin, "omp"), fakeOmp, { mode: 0o755 });
    process.env.PATH = bin + ":" + previousPath;
    process.env.FIXTURE_REPO = repo; process.env.FIXTURE_STATE = state_dir;
    const supervisor_credential_file = join(state_dir, "supervisor.token");
    writeFileSync(supervisor_credential_file, "supervisor-token", { mode: 0o600 });
    copyFileSync(join(here, "tesselate.ts"), join(ext, "tesselate.ts"));
    writeFileSync(join(ext, "tesselate.config.json"), JSON.stringify({ binary, state_dir, workspace_root, supervisor_credential_file }), { mode: 0o600 });
    const canonicalRepo = realpathSync(repo);
    const policyDir = join(state_dir, "projects", createHash("sha256").update(canonicalRepo).digest("hex"));
    mkdirSync(policyDir, { recursive: true, mode: 0o700 });
    writeFileSync(join(policyDir, "omp-provisioning.json"), JSON.stringify({
      version: 1, repository: canonicalRepo, integration: { copy: [".env", ".env.local", "packages/content/.revisions"] }
    }), { mode: 0o600 });
    const extension = (await import(pathToFileURL(join(ext, "tesselate.ts")).href)).default;
    let inputHandler: (event: { text: string; source: string }, ctx: unknown) => Promise<unknown> = async () => undefined;
    let tool: { execute: (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }> } | undefined;
    const messages: string[] = [];
    const notifications: string[] = [];
    const repairPrompts: string[] = [];
    const ui = { input: async (question: string) => question.startsWith("Acceptance") ? "feature.txt is committed" : "no shared-service tests",
      select: async (question: string, choices: string[]) => {
        if (question.includes("stopped:")) {
          repairPrompts.push(question);
          if (!repairReview) throw new Error(`Unexpected repair prompt: ${question}`);
          return choices[0];
        }
        return question.startsWith("Which repository") ? "sample" : question.startsWith("Resume") ? choices[0] : "main";
      },
      confirm: async () => true, notify: () => {} };
    let featureCommand: ((args: string, ctx: { cwd: string; ui: typeof ui }) => Promise<void>) | undefined;
    const resumeUi = { ...ui, input: async () => { throw new Error("Resume must not ask for a new specification."); },
      select: async (question: string, choices: string[]) => {
        if (question.startsWith("Which repository")) return "sample";
        if (question.startsWith("Which Tesselate feature")) return choices[0];
        if (question.includes("stopped:")) {
          repairPrompts.push(question);
          return choices[0];
        }
        throw new Error(`Unexpected resume selection: ${question}`);
      }, notify: (message: string) => { notifications.push(message); } };
    const pi = { zod: { object: () => ({}), string: () => ({ optional: () => ({}) }) }, on: (event: string, handler: typeof inputHandler) => { if (event === "input") inputHandler = handler; },
      registerCommand: (name: string, definition: { handler: NonNullable<typeof featureCommand> }) => { if (name === "feature") featureCommand = definition.handler; },
      registerTool: (definition: typeof tool) => { tool = definition; },
      sendUserMessage: (message: string) => { messages.push(message); } };
    // The test intentionally imports the installed extension by its runtime-selected user path.
    extension(pi as unknown as Parameters<typeof extension>[0]);
    assert.equal(await inputHandler({ source: "interactive", text: "fix the bug" }, { cwd: workspace_root, ui }), undefined);
    assert.equal(messages.length, 0);
    const result = await inputHandler({ source: "interactive", text: "Build a feature for this repo that writes feature.txt" }, { cwd: workspace_root, ui });
    assert.equal((result as { handled?: boolean } | undefined)?.handled, true);
    assert.match(messages[0], /feature\.txt is committed/);
    assert.match(messages[0], /no shared-service tests/);
    assert.ok(tool);
    assert.ok(featureCommand);
    const proposal = { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "no shared-service tests", validation_command: "git diff --exit-code HEAD -- README.md" };
    const missing = await tool!.execute("call", { ...proposal, validation_command: "" }, undefined, undefined, { cwd: workspace_root, ui: {
      ...ui, input: async () => "", confirm: async () => { throw new Error("Unproposed validation must not start a run"); }
    } });
    assert.match(missing.content[0].text, /No validation command selected/);
    const cancelled = await tool!.execute("call", { ...proposal, validation_command: undefined }, undefined, undefined, { cwd: workspace_root, ui: {
      ...ui, input: async () => undefined, confirm: async () => { throw new Error("Cancelled validation must not start a run"); }
    } });
    assert.match(cancelled.content[0].text, /cancelled; no run started/);
    const unsafe = await tool!.execute("call", { ...proposal, validation_command: "git push" }, undefined, undefined, { cwd: workspace_root, ui: {
      ...ui, input: async () => { throw new Error("A proposed build should not prompt for a command"); },
      confirm: async () => { throw new Error("Rejected validation must not start a run"); },
    } });
    assert.match(unsafe.content[0].text, /no shell chaining, push, merge, or shared services.*No run started/);
    const confirmations: string[] = [];
    process.env.FIXTURE_FAIL_AFTER_RUN_CREATE = "1";
    const modelContext = workerModel ? { model: { provider: workerModel.split("/")[0], id: workerModel.split("/").slice(1).join("/") } } : {};
    const interrupted = await tool!.execute("call", proposal, undefined, undefined, { cwd: workspace_root, ...modelContext, ui: {
      ...ui, input: async () => { throw new Error("A proposed build should not prompt for a command"); }, confirm: async (question: string) => {
        confirmations.push(question);
        return true;
      }
    } });
    assert.match(interrupted.content[0].text, /paused/);
    assert.ok(confirmations.some(question => /Validation in the integration worktree: git diff --exit-code HEAD -- README\.md/.test(question)));
    assert.ok(confirmations.some(question => /Copy Git-ignored local paths into the integration worktree only/.test(question) && /Missing optional: .env.local/.test(question)));
    assert.ok(existsSync(join(state_dir, "omp-runs", "intents")), interrupted.content[0].text);
    assert.equal(readdirSync(join(state_dir, "omp-runs", "intents")).length, 1);
    delete process.env.FIXTURE_FAIL_AFTER_RUN_CREATE;
    const interruptedRun = JSON.parse(readFileSync(join(state_dir, "fixture-db.json"), "utf8")).run.id;
    const workerLog = join(state_dir, "worker-launches.jsonl");
    const readLaunches = () => readFileSync(workerLog, "utf8").trim().split("\n").map(line => JSON.parse(line));
    process.env.FIXTURE_BAD_PLANNER = "1";
    await featureCommand!("resume", { cwd: workspace_root, ui: resumeUi });
    assert.ok(notifications.some(message => /Planner B did not produce a valid proposal/.test(message)));
    const proposals = readLaunches();
    assert.deepEqual(proposals.map(worker => worker.role), ["planner", "planner"]);
    assert.equal(new Set(proposals.map(worker => worker.cwd)).size, 2);
    assert.ok(proposals.every(worker => worker.start === mainHead && worker.readOnly));
    delete process.env.FIXTURE_BAD_PLANNER;
    notifications.length = 0;
    // A second interruption after role-token persistence replays issuance without the secret.
    const { startFeature } = await import(pathToFileURL(join(ext, "tesselate.ts")).href);
    const config = { binary, state_dir, workspace_root, supervisor_credential_file };
    const spec = { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "", validation_command: "git diff --exit-code HEAD -- README.md", base_branch: "" };
    process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE = "1";
    await featureCommand!("resume", { cwd: workspace_root, ui: resumeUi });
    assert.ok(notifications.some(message => /Tesselate paused/.test(message)));
    notifications.length = 0;
    delete process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE;
    process.env.FIXTURE_FAIL_AFTER_CAPTURE = "1";
    await assert.rejects(startFeature(config, repo, spec, ui), /exited 79/);
    const interruptedRecord = JSON.parse(readFileSync(join(state_dir, "omp-runs", interruptedRun, "record.json"), "utf8"));
    const heartbeatDir = join(state_dir, "omp-runs", interruptedRun, "heartbeat");
    assert.deepEqual(readLaunches().slice(2).map(worker => worker.role), ["planner", "plan selector", "coder"]);
    assert.equal(interruptedRecord.plan.recommendation, "B");
    assert.equal(interruptedRecord.plan.reason, "Plan B directly verifies the acceptance criterion.");
    const integrationEnv = join(state_dir, "omp-runs", interruptedRun, "integration", ".env");
    assert.equal(readFileSync(integrationEnv, "utf8"), "fixture-secret=not-for-reviewers\n");
    writeFileSync(integrationEnv, "modified locally\n");
    await assert.rejects(startFeature(config, repo, spec, ui), /Existing provisioned file differs/);
    writeFileSync(integrationEnv, "fixture-secret=not-for-reviewers\n");
    delete process.env.FIXTURE_FAIL_AFTER_CAPTURE;
    if (trackIgnored) {
      await featureCommand!(`resume ${interruptedRun}`, { cwd: repo, ui: resumeUi });
      assert.ok(notifications.some(message => /Reviewed commits track a provisioned local path/.test(message)));
      assert.equal(JSON.parse(readFileSync(join(heartbeatDir, "supervisor.json"), "utf8")).state, "failed");
      assert.equal(readFileSync(integrationEnv, "utf8"), "fixture-secret=not-for-reviewers\n");
      assert.equal(git(join(state_dir, "omp-runs", interruptedRun, "integration"), "rev-parse", "HEAD"), mainHead);
      return;
    }
    if (trailingReview) {
      process.env.FIXTURE_TRAILING_REVIEW = "1";
      await featureCommand!("resume", { cwd: repo, ui: resumeUi });
      assert.ok(notifications.some(message => /Reviewer 1 did not end with valid JSON; inspect/.test(message)));
      assert.equal(JSON.parse(readFileSync(join(heartbeatDir, "supervisor.json"), "utf8")).state, "failed");
      const paused = JSON.parse(readFileSync(join(state_dir, "omp-runs", interruptedRun, "record.json"), "utf8"));
      assert.equal(paused.phase, "review");
      assert.equal(JSON.parse(readFileSync(join(state_dir, "fixture-db.json"), "utf8")).reviews.length, 0);
      assert.equal(readLaunches().filter(worker => worker.role === "reviewer").length, 1);
      return;
    }
    if (retryReview) {
      process.env.FIXTURE_RETRY_REVIEW = "1";
      await featureCommand!("resume", { cwd: workspace_root, ui: resumeUi });
      assert.ok(notifications.some(message => /usage limit reached/.test(message)));
      notifications.length = 0;
      delete process.env.FIXTURE_RETRY_REVIEW;
    }
    await featureCommand!("resume", { cwd: workspace_root, ui: resumeUi });
    assert.ok(notifications.some(message => message.includes(`Resuming Tesselate run ${interruptedRun}`)));
    assert.ok(notifications.some(message => message.includes(`Tesselate run ${interruptedRun}: done`)));
    assert.equal(JSON.parse(readFileSync(join(state_dir, "fixture-db.json"), "utf8")).runCreateCount, 1);
    if (retryReview) assert.ok(readdirSync(join(state_dir, "omp-runs", interruptedRun)).some(name => name.startsWith("reviewer-0-round-1.log.attempt-")));
    const record = JSON.parse(readFileSync(join(state_dir, "omp-runs", interruptedRun, "record.json"), "utf8"));
    assert.equal(record.phase, "done");
    const completion = JSON.parse(readFileSync(join(state_dir, "omp-runs", interruptedRun, "completion.json"), "utf8"));
    assert.equal(completion.phase, "done");
    assert.match(completion.summary, /Tesselate run .*: done/);
    assert.ok(messages.some(message => message.includes(`Tesselate run ${interruptedRun}: done`)));
    notifications.length = 0;
    await featureCommand!("status", { cwd: workspace_root, ui: resumeUi });
    assert.ok(notifications.some(message => message.includes(`Tesselate run ${interruptedRun}: done`)));
    assert.equal(git(repo, "rev-parse", "main"), mainHead);
    assert.equal(git(record.integration_dir, "rev-parse", "HEAD"), record.last_head);
    assert.equal(statSync(integrationEnv).mode & 0o077, 0);
    assert.equal(readFileSync(join(record.integration_dir, "packages/content/.revisions/snapshot.json"), "utf8"), "{\"id\":1}\n");
    assert.doesNotMatch(readFileSync(join(state_dir, "omp-runs", interruptedRun, "record.json"), "utf8"), /fixture-secret/);
    const launches = readLaunches();
    assert.deepEqual(launches.slice(4).map(item => item.role), retryReview
      ? ["coder", "reviewer", "reviewer", "reviewer", "coder", "reviewer", "reviewer"]
      : ["coder", "reviewer", "reviewer", "coder", "reviewer", "reviewer"]);
    const [initialCoder, revisionCoder] = launches.filter(worker => worker.role === "coder");
    assert.notEqual(initialCoder.cwd, revisionCoder.cwd);
    assert.equal(revisionCoder.cwd, record.workspace_dir);
    assert.ok(launches.filter(item => item.role === "reviewer").every(item => item.readOnly && item.cwd !== record.workspace_dir && item.workerEnv === "1"));
    assert.ok(launches.every(item => !existsSync(join(item.cwd, ".env")) && !existsSync(join(item.cwd, "packages/content/.revisions"))));
    if (workerModel) assert.ok(launches.every(item => item.model === workerModel));
    assert.ok(existsSync(join(record.workspace_dir, ".workmux-local", "completion.md")));
    const heartbeatNames = readdirSync(heartbeatDir).filter(name => name.endsWith(".json")).sort();
    assert.deepEqual(heartbeatNames, ["coder-round-1.json", "coder-round-2.json", "plan-selector.json", "planner-a.json", "planner-b.json", "reviewer-0-round-1.json", "reviewer-0-round-2.json", "reviewer-1-round-1.json", "reviewer-1-round-2.json", "supervisor.json"]);
    const supervisorHeartbeat = JSON.parse(readFileSync(join(heartbeatDir, "supervisor.json"), "utf8"));
    assert.equal(supervisorHeartbeat.state, "finished");
    assert.equal(supervisorHeartbeat.role, "supervisor");
    assert.equal(supervisorHeartbeat.run_id, interruptedRun);
    const coderHeartbeat = JSON.parse(readFileSync(join(heartbeatDir, "coder-round-2.json"), "utf8"));
    assert.equal(coderHeartbeat.state, "finished");
    assert.equal(coderHeartbeat.role, "coder");
    assert.equal(coderHeartbeat.assignment, "revise Write feature.txt · round 2");
    const reviewerHeartbeat = JSON.parse(readFileSync(join(heartbeatDir, "reviewer-1-round-1.json"), "utf8"));
    assert.equal(reviewerHeartbeat.role, "reviewer");
    assert.equal(reviewerHeartbeat.assignment, "review · round 1 · reviewer 2/2");
    for (const name of heartbeatNames) assert.doesNotMatch(readFileSync(join(heartbeatDir, name), "utf8"), /fixture-secret/);
    const database = JSON.parse(readFileSync(join(state_dir, "fixture-db.json"), "utf8"));
    assert.equal(database.rounds.length, 2);
    assert.equal(database.dispositions[0].disposition, "fixed");
    if (repairReview) {
      assert.equal(repairPrompts.length, 1);
      assert.match(repairPrompts[0], /Review round or coding retry budget exhausted/);
      assert.equal(database.repairs, 1);
      assert.equal(database.run.limits.max_review_rounds, 2);
    } else assert.equal(database.repairs, undefined);
    assert.equal(database.task.submitted_commits[0].commit_id, record.last_head);
    assert.equal(new Set(database.reviews.map((review: { reviewer: string }) => review.reviewer)).size, 2);
    // Reopening a completed run reads the durable handoff; no worker starts again.
    const resumed = await startFeature({ binary, state_dir, workspace_root, supervisor_credential_file }, repo,
      { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed", constraints: "", validation_command: "git diff --exit-code HEAD -- README.md", base_branch: "" }, ui);
    assert.equal(resumed.run_id, record.run_id);
    assert.deepEqual(readLaunches(), launches);
    assert.equal(database.runCreateCount, 1);
  } finally {
    if (previousPath === undefined) delete process.env.PATH; else process.env.PATH = previousPath;
    if (previousRepo === undefined) delete process.env.FIXTURE_REPO; else process.env.FIXTURE_REPO = previousRepo;
    if (previousState === undefined) delete process.env.FIXTURE_STATE; else process.env.FIXTURE_STATE = previousState;
    if (previousCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_RUN_CREATE; else process.env.FIXTURE_FAIL_AFTER_RUN_CREATE = previousCrash;
    if (previousIssueCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE; else process.env.FIXTURE_FAIL_AFTER_CODER_ISSUE = previousIssueCrash;
    if (previousCaptureCrash === undefined) delete process.env.FIXTURE_FAIL_AFTER_CAPTURE; else process.env.FIXTURE_FAIL_AFTER_CAPTURE = previousCaptureCrash;
    if (previousTracked === undefined) delete process.env.FIXTURE_TRACK_IGNORED; else process.env.FIXTURE_TRACK_IGNORED = previousTracked;
    if (previousTrailing === undefined) delete process.env.FIXTURE_TRAILING_REVIEW; else process.env.FIXTURE_TRAILING_REVIEW = previousTrailing;
    if (previousRetry === undefined) delete process.env.FIXTURE_RETRY_REVIEW; else process.env.FIXTURE_RETRY_REVIEW = previousRetry;
    if (previousLowBudget === undefined) delete process.env.FIXTURE_LOW_REVIEW_BUDGET; else process.env.FIXTURE_LOW_REVIEW_BUDGET = previousLowBudget;
    if (previousBadPlanner === undefined) delete process.env.FIXTURE_BAD_PLANNER; else process.env.FIXTURE_BAD_PLANNER = previousBadPlanner;
    rmSync(root, { recursive: true, force: true });
  }
}

// Real coordinator and Git; only the model subprocesses have deterministic fixture answers.
export async function runComparisonSmoke(binary: string, rejectA = false): Promise<void> {
  binary = resolve(binary);
  const root = realpathSync(mkdtempSync(join(tmpdir(), "tesselate-compare-")));
  const repo = join(root, "sample"), state_dir = join(root, "state"), bin = join(root, "bin");
  const env = { PATH: `${bin}:${process.env.PATH}`, TESSELATE_WORKSPACE_ROOT: root, FIXTURE_STATE: state_dir,
    FIXTURE_REJECT_CANDIDATE_A: rejectA ? "1" : "0" };
  const previous = Object.fromEntries(Object.keys(env).map(key => [key, process.env[key]]));
  Object.assign(process.env, env);
  try {
    mkdirSync(bin); mkdirSync(state_dir);
    writeFileSync(join(bin, "omp"), fakeOmp, { mode: 0o755 });
    execFileSync("git", ["init", "-q", "-b", "main", repo]);
    writeFileSync(join(repo, "README.md"), "Original\n");
    writeFileSync(join(repo, ".gitignore"), ".workmux-local/\n");
    const presets = join(repo, ".tesselate.toml");
    const validation = "git diff --exit-code main HEAD -- README.md";
    writeFileSync(presets, `default_workflow = "standard"
[workflows.experiment]
candidates = 2
reviewers_per_candidate = 2
comparison_reviewers = 1
winner_approval = "human"

[validation]
command = "${rejectA ? "git status --short" : validation}"
`);
    git(repo, "add", ".");
    git(repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "Base");
    const base = git(repo, "rev-parse", "HEAD");
    const cli = (...args: string[]) => execFileSync(binary, ["--state-dir", state_dir, ...args], { encoding: "utf8" });
    const { credential_file } = JSON.parse(cli("credential", "bootstrap", "--actor-name", "test-supervisor"));
    const extPath = join(root, "tesselate.ts");
    copyFileSync(join(here, "tesselate.ts"), extPath);
    writeFileSync(join(root, "tesselate.config.json"), JSON.stringify({ binary, state_dir, workspace_root: root, supervisor_credential_file: credential_file }));
    const ext = (await import(pathToFileURL(extPath).href)).default;
    const ui = {
      input: async (question: string) => {
        if (question.startsWith("Acceptance")) return "feature.txt is committed";
        if (question.startsWith("Constraints")) return "";
        throw new Error(`A configured build should not prompt for a command: ${question}`);
      },
      select: async (question: string, choices: string[]) => {
        if (question.startsWith("Use a local")) return "main";
        if (question.includes("Which candidate")) {
          assert.match(question, rejectA ? /Only candidate B passed/ : /Comparison recommends A: Fixture recommends A over B/);
          assert.equal(choices.some(choice => choice.startsWith("Integrate candidate A")), !rejectA);
          return choices[0]; // Leave both commits unintegrated until a later OMP session approves one.
        }
        if (choices.includes("main")) return "main";
        throw new Error(`Unexpected selection: ${question}`);
      },
      confirm: async (question: string) => {
        assert.ok(question.includes(`: ${validation}\n`), question);
        return true;
      },
      notify: (message: string, level: string) => { if (level === "error") throw new Error(message); },
    };
    type Feature = (args: string, ctx: { cwd: string; ui: typeof ui }) => Promise<void>;
    type Start = (...args: unknown[]) => Promise<{ content: Array<{ text: string }> }>;
    const register = () => {
      let feature!: Feature, start!: Start;
      ext({
        zod: { object: () => ({}), string: () => ({ optional: () => ({}) }) },
        on: () => {}, registerCommand: (_: string, definition: { handler: Feature }) => { feature = definition.handler; },
        registerTool: (definition: { execute: Start }) => { start = definition.execute; },
        sendUserMessage: (message: string) => {
          if (/selected workflow "experiment"/.test(message)) return;
          assert.match(message, /Tesselate run .*: (done|paused)/);
        },
      } as unknown as Parameters<typeof ext>[0]);
      return { feature, start };
    };
    const session = register();
    await session.feature("--workflow experiment Write feature.txt", { cwd: repo, ui });
    const result = await session.start("call", { goal: "Write feature.txt", acceptance_criteria: "feature.txt is committed",
      constraints: "", validation_command: "false", ...(rejectA ? { validation_override: validation } : {}) },
      undefined, undefined, { cwd: repo, ui });
    assert.match(result.content[0].text, /run ([0-9a-f-]+): comparing/);
    const runId = result.content[0].text.match(/run ([0-9a-f-]+): comparing/)![1];
    const handoff = join(state_dir, "omp-runs", runId, "record.json");
    const before = JSON.parse(readFileSync(handoff, "utf8"));
    assert.equal(git(before.integration_dir, "rev-parse", "HEAD"), base);
    const launchesPath = join(state_dir, "worker-launches.jsonl");
    const launches = readFileSync(launchesPath, "utf8");
    const workers = launches.trim().split("\n").map(line => JSON.parse(line));
    const coderA = workers.filter(worker => worker.role === "coder" && worker.slot === "a");
    const coderB = workers.filter(worker => worker.role === "coder" && worker.slot === "b");
    assert.equal(coderA.length, 2); // A alone needs a revision.
    assert.equal(coderB.length, 1);
    assert.equal(coderA[0].start, base);
    assert.equal(coderB[0].start, base);
    assert.notEqual(coderA[0].cwd, coderB[0].cwd);
    assert.equal(workers.filter(worker => worker.role === "comparison").length, rejectA ? 0 : 1);
    assert.ok(workers.filter(worker => worker.role !== "coder").every(worker => worker.readOnly));
    assert.match(cli("status"), /paused · nothing integrated/);
    writeFileSync(presets, '[validation]\ncommand = "false"\n'); // Resuming must validate integration with the approved command.
    await register().feature(`resume ${runId}`, { cwd: repo, ui: {
      ...ui, input: async () => { throw new Error("Resume asked for a new spec"); },
      select: async (_question, choices) => choices.find(choice => choice.startsWith("Integrate candidate B"))!,
    } });
    const after = JSON.parse(readFileSync(handoff, "utf8"));
    assert.equal(after.phase, "done");
    assert.equal(git(after.integration_dir, "rev-parse", "HEAD"), before.comparison.lanes[1].last_head);
    assert.equal(git(repo, "rev-parse", "main"), base);
    assert.equal(readFileSync(launchesPath, "utf8"), launches);
    const runs = JSON.parse(cli("--credential-file", credential_file, "call", "tesselate.v1.runs.list",
      "--params", JSON.stringify({ project_id: after.project_id, page: { limit: 20, after: null } })).trim());
    const completed = runs.items.find((run: { id: string }) => run.id === runId);
    assert.equal(completed.status, "completed");
    assert.equal(completed.comparison.winner.task_id, before.comparison.lanes[1].task_id);
    assert.deepEqual(completed.reviewed_commits.map((commit: { commit_id: string }) => commit.commit_id), [before.comparison.lanes[1].last_head]);
    const status = cli("status");
    assert.match(status, /complete · selected B/);
    if (!rejectA) assert.match(status, /comparison\s+compare candidates A and B/);
    console.log(status.trimEnd());
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    rmSync(root, { recursive: true, force: true });
  }
}
