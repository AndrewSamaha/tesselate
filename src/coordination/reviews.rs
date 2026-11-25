//! Independent review requests, results, dispositions, and task/run completion policy (F06).
use super::tasks::{ReviewCompletionGate, TaskEngine, TaskRecord};
use crate::{
    core::*,
    persistence::{
        Database, EntityKind, EntityScope, MutationInput, NewEvent, PersistenceError, StateChange,
        StatePrecondition,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewPolicy {
    pub min_independent_reviewers: u32,
    pub high_severity_defer_requires_human: bool,
}

impl ReviewPolicy {
    fn validate(&self) -> CoreResult<()> {
        if self.min_independent_reviewers < 2 {
            return Err(CoreError::Invalid(
                "review policy requires at least two independent reviewers".into(),
            ));
        }
        Ok(())
    }
}

/// F01 provides a validated project policy snapshot; F06 owns review satisfaction semantics.
pub trait ProjectReviewPolicy: Send + Sync {
    fn review_policy(&self, project_id: ProjectId) -> CoreResult<ReviewPolicy>;
}

/// F03 maps durable submitted commit references to their authoring Tesselate actors.
pub trait CommitAuthorLookup: Send + Sync {
    fn authors(&self, commits: &[CommitRef]) -> CoreResult<HashSet<ActorId>>;
    fn ensure_comparison_capture(&self, task_id: TaskId, commits: &[CommitRef]) -> CoreResult<()>;
}

/// F07 enforces approval when project policy gates a high-severity deferral.
pub trait ReviewHumanGate: Send + Sync {
    fn allow_high_severity_deferral(
        &self,
        ctx: &SessionContext,
        project_id: ProjectId,
        run_id: RunId,
        task_id: TaskId,
        finding: &ReviewFinding,
        reason: &str,
    ) -> CoreResult<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FindingRecord {
    project_id: ProjectId,
    run_id: RunId,
    task_id: TaskId,
    round_id: ReviewRoundId,
    review_id: ReviewId,
    finding: ReviewFinding,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DispositionRecord {
    project_id: ProjectId,
    run_id: RunId,
    task_id: TaskId,
    round_id: ReviewRoundId,
    disposition: FindingDisposition,
}

pub struct ReviewCoordinator {
    db: Database,
    policy: Arc<dyn ProjectReviewPolicy>,
    authors: Arc<dyn CommitAuthorLookup>,
    human: Arc<dyn ReviewHumanGate>,
}

impl ReviewCoordinator {
    pub fn new(
        db: Database,
        policy: Arc<dyn ProjectReviewPolicy>,
        authors: Arc<dyn CommitAuthorLookup>,
        human: Arc<dyn ReviewHumanGate>,
    ) -> Self {
        Self {
            db,
            policy,
            authors,
            human,
        }
    }

    /// Creates the review round and transitions Submitted -> InReview in one F02 mutation.
    pub fn request_review(
        &self,
        ctx: &SessionContext,
        request: ReviewRequest,
        key: IdempotencyKey,
        tasks: &TaskEngine,
    ) -> CoreResult<Mutation<ReviewRound>> {
        const OP: &str = "reviews.request";
        if let Some(prior) = self.prior(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        let (task_record, _) = self.load_task(request.task_id)?;
        if task_record.task.status != TaskStatus::Submitted {
            return Err(CoreError::Invalid(
                "reviews can only be requested for a submitted task".into(),
            ));
        }
        let (run, _) = self.load_run(task_record.task.run_id)?;
        if run.status != RunStatus::Active {
            return Err(CoreError::Invalid("run is not active".into()));
        }
        validate_commit_refs(&self.db, run.project_id, &request.commit_refs)?;
        let submitted = &task_record.task.submitted_commits;
        let matches_submission = if run.comparison.is_some() {
            submitted == &request.commit_refs
        } else {
            same_refs(submitted, &request.commit_refs)
        };
        if !matches_submission {
            return Err(CoreError::Invalid(
                "review refs must match the task's submitted commits".into(),
            ));
        }
        if run.comparison.is_some() {
            self.authors
                .ensure_comparison_capture(request.task_id, submitted)?;
        }
        let rounds = self.rounds_for_task(request.task_id)?;
        let next_round = rounds.iter().map(|round| round.round).max().unwrap_or(0) + 1;
        if next_round > run.limits.max_review_rounds {
            return Err(CoreError::Recoverable(
                "maximum review rounds reached".into(),
            ));
        }
        self.validate_previous_findings(request.task_id, &rounds, &request.previous_finding_ids)?;
        let round = ReviewRound {
            id: ReviewRoundId::new(),
            task_id: request.task_id,
            round: next_round,
            commit_refs: request.commit_refs.clone(),
            previous_finding_ids: request.previous_finding_ids.clone(),
        };
        let (prepared, mut changes) = tasks.prepare_review_transition(
            ctx,
            request.task_id,
            TaskStatus::InReview,
            "review requested",
        )?;
        if run.comparison.is_some() && prepared.submitted_commits != round.commit_refs {
            return Err(CoreError::Recoverable(
                "comparison candidate submission changed; retry review request".into(),
            ));
        }
        changes.push(
            StateChange::insert(
                EntityKind::ReviewRound,
                round.id.0.to_string(),
                scope(run.project_id, run.id, request.task_id),
                &round,
            )
            .map_err(storage_error)?,
        );
        let event = event(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            "review.round_requested",
            json!({"round":round.round,"commit_refs":round.commit_refs,"previous_finding_ids":round.previous_finding_ids}),
        );
        self.commit(ctx, OP, &key, &request, &round, changes, vec![], event)
    }

    /// Stores an explicit reviewer result and all findings. The round revision serializes
    /// concurrent submissions; the task revision fence rejects writes after completion.
    pub fn submit_review(
        &self,
        ctx: &SessionContext,
        request: SubmitReview,
        key: IdempotencyKey,
    ) -> CoreResult<Mutation<Review>> {
        const OP: &str = "reviews.submit";
        if let Some(prior) = self.prior(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Reviewer)?;
        if request.summary.trim().is_empty() || request.passed == !request.findings.is_empty() {
            return Err(CoreError::Invalid(
                "review must be an explicit pass without findings or findings without a pass"
                    .into(),
            ));
        }
        let (round, round_revision) = self.load_round(request.round_id)?;
        let (task_record, task_revision) = self.load_task(round.task_id)?;
        if task_record.task.status != TaskStatus::InReview
            || !same_refs(&task_record.task.submitted_commits, &round.commit_refs)
        {
            return Err(CoreError::Invalid(
                "task is no longer accepting reviews for this round".into(),
            ));
        }
        let (run, _) = self.load_run(task_record.task.run_id)?;
        let reviewer = self
            .db
            .load::<Actor>(EntityKind::Actor, &ctx.actor_id().0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?
            .0;
        if reviewer.role != Role::Reviewer {
            return Err(CoreError::Forbidden);
        }
        let authors = self.authors.authors(&round.commit_refs)?;
        if authors.contains(&ctx.actor_id()) {
            return Err(CoreError::Forbidden);
        }
        let existing = self.reviews_for_round(round.id)?;
        if existing
            .iter()
            .any(|review| review.reviewer == ctx.actor_id())
        {
            return Err(CoreError::Invalid(
                "reviewer already submitted a result for this round".into(),
            ));
        }
        let mut finding_ids = HashSet::new();
        for finding in &request.findings {
            if !finding_ids.insert(finding.id)
                || finding.location.trim().is_empty()
                || finding.evidence.trim().is_empty()
                || finding.proposed_correction.trim().is_empty()
            {
                return Err(CoreError::Invalid("finding IDs must be unique and each finding needs location, evidence, and a proposed correction".into()));
            }
            if self
                .db
                .load::<FindingRecord>(EntityKind::ReviewFinding, &finding.id.0.to_string())
                .map_err(storage_error)?
                .is_some()
            {
                return Err(CoreError::Invalid("finding ID already exists".into()));
            }
        }
        let draft_event = event(
            ctx,
            run.project_id,
            run.id,
            Some(round.task_id),
            "review.submitted",
            json!({"round_id":round.id,"passed":request.passed,"finding_count":request.findings.len()}),
        );
        let review = Review {
            id: ReviewId::new(),
            round_id: round.id,
            task_id: round.task_id,
            reviewer: ctx.actor_id(),
            passed: request.passed,
            findings: request.findings.clone(),
            event_id: draft_event.event_id,
        };
        let mut changes = vec![
            StateChange::update(
                EntityKind::ReviewRound,
                round.id.0.to_string(),
                scope(run.project_id, run.id, round.task_id),
                round_revision,
                &round,
            )
            .map_err(storage_error)?,
            StateChange::insert(
                EntityKind::Review,
                review.id.0.to_string(),
                review_scope(run.project_id, run.id, round.task_id, ctx.actor_id()),
                &review,
            )
            .map_err(storage_error)?,
        ];
        for finding in &request.findings {
            let record = FindingRecord {
                project_id: run.project_id,
                run_id: run.id,
                task_id: round.task_id,
                round_id: round.id,
                review_id: review.id,
                finding: finding.clone(),
            };
            changes.push(
                StateChange::insert(
                    EntityKind::ReviewFinding,
                    finding.id.0.to_string(),
                    review_scope(run.project_id, run.id, round.task_id, ctx.actor_id()),
                    &record,
                )
                .map_err(storage_error)?,
            );
        }
        let preconditions = vec![StatePrecondition {
            kind: EntityKind::Task,
            id: round.task_id.0.to_string(),
            expected_revision: task_revision,
        }];
        self.commit(
            ctx,
            OP,
            &key,
            &request,
            &review,
            changes,
            preconditions,
            draft_event,
        )
    }

    /// Adds new finding dispositions. When every finding is dispositioned and at least one
    /// requires a fix, F04 validates and commits InReview -> RevisionRequired in this batch.
    pub fn disposition_findings(
        &self,
        ctx: &SessionContext,
        request: DispositionFindings,
        key: IdempotencyKey,
        tasks: &TaskEngine,
    ) -> CoreResult<Mutation<Task>> {
        const OP: &str = "reviews.disposition";
        if let Some(prior) = self.prior(ctx, OP, &key, &request)? {
            return Ok(prior);
        }
        require_role(ctx, Role::Supervisor)?;
        if request.dispositions.is_empty() {
            return Err(CoreError::Invalid(
                "at least one finding disposition is required".into(),
            ));
        }
        let (task_record, task_revision) = self.load_task(request.task_id)?;
        if task_record.task.status != TaskStatus::InReview {
            return Err(CoreError::Invalid(
                "findings can only be dispositioned while a task is in review".into(),
            ));
        }
        let (run, run_revision) = self.load_run(task_record.task.run_id)?;
        if run.status != RunStatus::Active {
            return Err(CoreError::Invalid("run is not active".into()));
        }
        let findings = self.findings_for_task(request.task_id)?;
        let existing = self.dispositions_for_task(request.task_id)?;
        let existing_ids = existing
            .iter()
            .map(|d| d.disposition.finding_id)
            .collect::<HashSet<_>>();
        let mut request_ids = HashSet::new();
        for disposition in &request.dispositions {
            if !request_ids.insert(disposition.finding_id)
                || existing_ids.contains(&disposition.finding_id)
            {
                return Err(CoreError::Invalid("finding dispositions must be unique and cannot replace an existing disposition".into()));
            }
            if !findings.contains_key(&disposition.finding_id) {
                return Err(CoreError::NotFound);
            }
            if disposition.reason.trim().is_empty() {
                return Err(CoreError::Invalid(
                    "every finding disposition requires a reason".into(),
                ));
            }
        }
        let policy = self.project_policy(run.project_id)?;
        for disposition in &request.dispositions {
            if disposition.disposition == DispositionKind::Deferred
                && policy.high_severity_defer_requires_human
            {
                let finding = &findings[&disposition.finding_id].finding;
                if matches!(
                    finding.severity,
                    FindingSeverity::High | FindingSeverity::Critical
                ) {
                    self.human.allow_high_severity_deferral(
                        ctx,
                        run.project_id,
                        run.id,
                        request.task_id,
                        finding,
                        &disposition.reason,
                    )?;
                }
            }
        }
        let all_ids = findings.keys().copied().collect::<HashSet<_>>();
        let disposed_ids = existing_ids
            .union(&request_ids)
            .copied()
            .collect::<HashSet<_>>();
        let all_disposed = all_ids == disposed_ids;
        let needs_revision = all_disposed
            && existing
                .iter()
                .map(|d| &d.disposition)
                .chain(request.dispositions.iter())
                .any(|d| d.disposition == DispositionKind::Fixed);
        let mut changes = Vec::new();
        let mut rounds_to_bump = HashMap::<ReviewRoundId, (ReviewRound, i64)>::new();
        for disposition in &request.dispositions {
            let record = &findings[&disposition.finding_id];
            let disp_record = DispositionRecord {
                project_id: run.project_id,
                run_id: run.id,
                task_id: request.task_id,
                round_id: record.round_id,
                disposition: disposition.clone(),
            };
            changes.push(
                StateChange::insert(
                    EntityKind::FindingDisposition,
                    disposition.finding_id.0.to_string(),
                    review_scope(run.project_id, run.id, request.task_id, ctx.actor_id()),
                    &disp_record,
                )
                .map_err(storage_error)?,
            );
            if !rounds_to_bump.contains_key(&record.round_id) {
                rounds_to_bump.insert(record.round_id, self.load_round(record.round_id)?);
            }
        }
        for (round_id, (round, revision)) in rounds_to_bump {
            changes.push(
                StateChange::update(
                    EntityKind::ReviewRound,
                    round_id.0.to_string(),
                    scope(run.project_id, run.id, request.task_id),
                    revision,
                    &round,
                )
                .map_err(storage_error)?,
            );
        }
        let (result, guards) = if needs_revision {
            let (task, changes_from_task) = tasks.prepare_review_transition(
                ctx,
                request.task_id,
                TaskStatus::RevisionRequired,
                "findings require revision",
            )?;
            changes.extend(changes_from_task);
            (task, Vec::new())
        } else {
            let guard = vec![
                StatePrecondition {
                    kind: EntityKind::Task,
                    id: request.task_id.0.to_string(),
                    expected_revision: task_revision,
                },
                StatePrecondition {
                    kind: EntityKind::Run,
                    id: run.id.0.to_string(),
                    expected_revision: run_revision,
                },
            ];
            (task_record.task, guard)
        };
        let draft_event = event(
            ctx,
            run.project_id,
            run.id,
            Some(request.task_id),
            "review.findings_dispositioned",
            json!({"dispositions":request.dispositions}),
        );
        self.commit(
            ctx,
            OP,
            &key,
            &request,
            &result,
            changes,
            guards,
            draft_event,
        )
    }

    pub fn list_reviews(
        &self,
        ctx: &SessionContext,
        request: ListReviews,
    ) -> CoreResult<Page<Review>> {
        require_read_role(ctx)?;
        self.load_task(request.task_id)?;
        self.db
            .list_entities(
                EntityKind::Review,
                &EntityScope {
                    task_id: Some(request.task_id.0.to_string()),
                    ..Default::default()
                },
                request.page,
            )
            .map_err(storage_error)
    }

    pub fn list_review_rounds(
        &self,
        ctx: &SessionContext,
        request: ListReviewRounds,
    ) -> CoreResult<Page<ReviewRound>> {
        require_read_role(ctx)?;
        self.load_task(request.task_id)?;
        self.db
            .list_entities(
                EntityKind::ReviewRound,
                &EntityScope {
                    task_id: Some(request.task_id.0.to_string()),
                    ..Default::default()
                },
                request.page,
            )
            .map_err(storage_error)
    }

    fn project_policy(&self, project_id: ProjectId) -> CoreResult<ReviewPolicy> {
        let policy = self.policy.review_policy(project_id)?;
        policy.validate()?;
        Ok(policy)
    }

    fn validate_previous_findings(
        &self,
        task_id: TaskId,
        rounds: &[ReviewRound],
        ids: &[FindingId],
    ) -> CoreResult<()> {
        let unique = ids.iter().copied().collect::<HashSet<_>>();
        if unique.len() != ids.len() {
            return Err(CoreError::Invalid(
                "previous finding IDs must be unique".into(),
            ));
        }
        let earlier_rounds = rounds.iter().map(|round| round.id).collect::<HashSet<_>>();
        let valid = self
            .findings_for_task(task_id)?
            .into_iter()
            .filter_map(|(id, record)| earlier_rounds.contains(&record.round_id).then_some(id))
            .collect::<HashSet<_>>();
        if ids.iter().any(|id| !valid.contains(id)) {
            return Err(CoreError::Invalid(
                "previous findings must belong to an earlier round of this task".into(),
            ));
        }
        let dispositions = self.dispositions_for_task(task_id)?;
        let fixed = dispositions
            .iter()
            .filter(|record| record.disposition.disposition == DispositionKind::Fixed)
            .map(|record| record.disposition.finding_id)
            .collect::<HashSet<_>>();
        if !fixed.is_subset(&unique) {
            return Err(CoreError::Invalid(
                "a revision round must reference every finding marked fixed".into(),
            ));
        }
        Ok(())
    }

    fn load_task(&self, task_id: TaskId) -> CoreResult<(TaskRecord, i64)> {
        self.db
            .load(EntityKind::Task, &task_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }
    fn load_run(&self, run_id: RunId) -> CoreResult<(Run, i64)> {
        self.db
            .load(EntityKind::Run, &run_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }
    fn load_round(&self, round_id: ReviewRoundId) -> CoreResult<(ReviewRound, i64)> {
        self.db
            .load(EntityKind::ReviewRound, &round_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)
    }
    fn rounds_for_task(&self, task_id: TaskId) -> CoreResult<Vec<ReviewRound>> {
        self.all_entities(
            EntityKind::ReviewRound,
            EntityScope {
                task_id: Some(task_id.0.to_string()),
                ..Default::default()
            },
        )
    }
    fn reviews_for_round(&self, round_id: ReviewRoundId) -> CoreResult<Vec<Review>> {
        let (round, _) = self.load_round(round_id)?;
        self.all_entities(
            EntityKind::Review,
            EntityScope {
                task_id: Some(round.task_id.0.to_string()),
                ..Default::default()
            },
        )
        .map(|reviews: Vec<Review>| {
            reviews
                .into_iter()
                .filter(|review| review.round_id == round_id)
                .collect()
        })
    }
    fn findings_for_task(&self, task_id: TaskId) -> CoreResult<HashMap<FindingId, FindingRecord>> {
        self.all_entities::<FindingRecord>(
            EntityKind::ReviewFinding,
            EntityScope {
                task_id: Some(task_id.0.to_string()),
                ..Default::default()
            },
        )?
        .into_iter()
        .map(|record| Ok((record.finding.id, record)))
        .collect()
    }
    fn dispositions_for_task(&self, task_id: TaskId) -> CoreResult<Vec<DispositionRecord>> {
        self.all_entities(
            EntityKind::FindingDisposition,
            EntityScope {
                task_id: Some(task_id.0.to_string()),
                ..Default::default()
            },
        )
    }
    fn all_entities<T: for<'de> Deserialize<'de>>(
        &self,
        kind: EntityKind,
        scope: EntityScope,
    ) -> CoreResult<Vec<T>> {
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = self
                .db
                .list_entities(kind, &scope, PageRequest { limit: 500, after })
                .map_err(storage_error)?;
            let len = page.items.len();
            all.extend(page.items);
            if len < 500 {
                break;
            }
            after = page.next_cursor.map(EventCursor);
        }
        Ok(all)
    }
    fn prior<Q: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
    ) -> CoreResult<Option<Mutation<R>>> {
        self.db
            .prior_mutation(ctx, operation, key, request)
            .map_err(storage_error)
    }
    fn commit<Q: Serialize, R: Serialize + for<'de> Deserialize<'de>>(
        &self,
        ctx: &SessionContext,
        operation: &str,
        key: &IdempotencyKey,
        request: &Q,
        result: &R,
        changes: Vec<StateChange>,
        preconditions: Vec<StatePrecondition>,
        event: NewEvent,
    ) -> CoreResult<Mutation<R>> {
        match self.db.commit_mutation(MutationInput {
            context: ctx,
            operation,
            idempotency_key: key,
            request,
            preconditions,
            result,
            changes,
            event,
        }) {
            Ok(mutation) => Ok(mutation),
            Err(PersistenceError::StateConflict(id)) => {
                if let Some(prior) = self.prior(ctx, operation, key, request)? {
                    return Ok(prior);
                }
                if self
                    .db
                    .load::<TaskRecord>(EntityKind::Task, &id)
                    .ok()
                    .flatten()
                    .is_some_and(|(record, _)| record.task.status == TaskStatus::Completed)
                {
                    return Err(CoreError::Invalid(
                        "task is no longer eligible for review changes".into(),
                    ));
                }
                Err(CoreError::Recoverable(
                    "review state changed concurrently; retry the operation".into(),
                ))
            }
            Err(error) => Err(storage_error(error)),
        }
    }
}

impl ReviewCompletionGate for ReviewCoordinator {
    fn ensure_task_complete(&self, task_id: TaskId) -> CoreResult<Vec<StatePrecondition>> {
        let (task_record, _) = self.load_task(task_id)?;
        let task = task_record.task;
        let (run, _) = self.load_run(task.run_id)?;
        let min_independent_reviewers = if let Some(comparison) = &run.comparison {
            comparison.workflow.reviewers_per_candidate
        } else {
            self.project_policy(run.project_id)?.min_independent_reviewers
        };
        if task.submitted_commits.is_empty() {
            return Err(CoreError::Recoverable(
                "task has no submitted commits to review".into(),
            ));
        }
        let rounds = self.rounds_for_task(task_id)?;
        let current = rounds
            .iter()
            .filter(|round| {
                if run.comparison.is_some() {
                    round.commit_refs == task.submitted_commits
                } else {
                    same_refs(&round.commit_refs, &task.submitted_commits)
                }
            })
            .max_by_key(|round| round.round)
            .ok_or_else(|| {
                CoreError::Recoverable("current submitted commits have no review round".into())
            })?;
        let reviews = self.reviews_for_round(current.id)?;
        let authors = self.authors.authors(&task.submitted_commits)?;
        let reviewers = reviews
            .iter()
            .map(|review| review.reviewer)
            .collect::<HashSet<_>>();
        if reviewers.iter().any(|reviewer| authors.contains(reviewer)) {
            return Err(CoreError::Forbidden);
        }
        if reviewers.len() < min_independent_reviewers as usize {
            return Err(CoreError::Recoverable(
                "insufficient independent reviewers".into(),
            ));
        }
        let findings = self.findings_for_task(task_id)?;
        let dispositions = self
            .dispositions_for_task(task_id)?
            .into_iter()
            .map(|record| (record.disposition.finding_id, record))
            .collect::<HashMap<_, _>>();
        if findings
            .keys()
            .any(|finding_id| !dispositions.contains_key(finding_id))
        {
            return Err(CoreError::Recoverable(
                "every review finding needs a supervisor disposition".into(),
            ));
        }
        for (finding_id, record) in &findings {
            if dispositions[finding_id].disposition.disposition == DispositionKind::Fixed {
                let source_round = rounds
                    .iter()
                    .find(|round| round.id == record.round_id)
                    .ok_or(CoreError::NotFound)?;
                if current.round <= source_round.round
                    || same_refs(&current.commit_refs, &source_round.commit_refs)
                    || !current.previous_finding_ids.contains(finding_id)
                {
                    return Err(CoreError::Recoverable(
                        "fixed findings require a later review round covering revised commits"
                            .into(),
                    ));
                }
            }
        }
        let mut guards = Vec::new();
        for round in &rounds {
            let (_, revision) = self.load_round(round.id)?;
            guards.push(StatePrecondition {
                kind: EntityKind::ReviewRound,
                id: round.id.0.to_string(),
                expected_revision: revision,
            });
        }
        Ok(guards)
    }

    fn reviewed_commits_for_run(&self, run_id: RunId) -> CoreResult<Vec<CommitRef>> {
        let tasks = self.all_entities::<TaskRecord>(
            EntityKind::Task,
            EntityScope {
                run_id: Some(run_id.0.to_string()),
                ..Default::default()
            },
        )?;
        let mut seen = HashSet::new();
        let mut commits = Vec::new();
        for record in tasks
            .into_iter()
            .filter(|record| record.task.status == TaskStatus::Completed)
        {
            self.ensure_task_complete(record.task.id)?;
            for commit in record.task.submitted_commits {
                if seen.insert(commit.clone()) {
                    commits.push(commit);
                }
            }
        }
        Ok(commits)
    }
}

fn validate_commit_refs(
    db: &Database,
    project_id: ProjectId,
    refs: &[CommitRef],
) -> CoreResult<()> {
    if refs.is_empty() {
        return Err(CoreError::Invalid(
            "review request must include submitted commits".into(),
        ));
    }
    let mut seen = HashSet::new();
    for commit in refs {
        if commit.commit_id.trim().is_empty() || !seen.insert(commit.clone()) {
            return Err(CoreError::Invalid(
                "commit refs must be unique and nonempty".into(),
            ));
        }
        let repo = db
            .load::<Repository>(EntityKind::Repository, &commit.repository_id.0.to_string())
            .map_err(storage_error)?
            .ok_or(CoreError::NotFound)?
            .0;
        if repo.project_id != project_id {
            return Err(CoreError::Invalid(
                "review repository must belong to the task's project".into(),
            ));
        }
    }
    Ok(())
}
fn same_refs(left: &[CommitRef], right: &[CommitRef]) -> bool {
    left.len() == right.len()
        && left.iter().collect::<HashSet<_>>() == right.iter().collect::<HashSet<_>>()
}
fn require_role(ctx: &SessionContext, role: Role) -> CoreResult<()> {
    if ctx.role() == role {
        Ok(())
    } else {
        Err(CoreError::Forbidden)
    }
}
fn require_read_role(ctx: &SessionContext) -> CoreResult<()> {
    if matches!(
        ctx.role(),
        Role::Supervisor | Role::Coder | Role::Reviewer | Role::Cli
    ) {
        Ok(())
    } else {
        Err(CoreError::Forbidden)
    }
}
fn scope(project_id: ProjectId, run_id: RunId, task_id: TaskId) -> EntityScope {
    EntityScope {
        project_id: Some(project_id.0.to_string()),
        run_id: Some(run_id.0.to_string()),
        task_id: Some(task_id.0.to_string()),
        owner_id: None,
    }
}
fn review_scope(
    project_id: ProjectId,
    run_id: RunId,
    task_id: TaskId,
    actor: ActorId,
) -> EntityScope {
    EntityScope {
        owner_id: Some(actor.0.to_string()),
        ..scope(project_id, run_id, task_id)
    }
}
fn event(
    ctx: &SessionContext,
    project_id: ProjectId,
    run_id: RunId,
    task_id: Option<TaskId>,
    event_type: &str,
    payload: serde_json::Value,
) -> NewEvent {
    NewEvent::from_context(
        ctx,
        ctx.actor_id().0.to_string(),
        EventScope {
            project_id: Some(project_id),
            run_id: Some(run_id),
            task_id,
        },
        event_type,
        payload,
    )
}
fn storage_error(error: PersistenceError) -> CoreError {
    match error {
        PersistenceError::IdempotencyConflict => CoreError::IdempotencyConflict,
        PersistenceError::StateConflict(id) => {
            CoreError::Recoverable(format!("state changed concurrently: {id}"))
        }
        PersistenceError::Invalid(message) => CoreError::Invalid(message),
        other => CoreError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests;
