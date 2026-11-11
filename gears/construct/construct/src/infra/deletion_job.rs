//! Where Deletion runs: the erasures the subjects ask for, on the instance
//! that took the request, and the leader's job, which runs again every
//! erasure that stopped and removes what is past the retention periods.

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cluster_sdk::{LeaderElectionCapability, LeaderElectionV1};
use futures::FutureExt as _;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{Id, JoinError, JoinSet};
use tokio_retry::strategy::jitter;
use tokio_util::sync::CancellationToken;
use toolkit::client_hub::ClientHub;
use uuid::Uuid;

use crate::domain::deletion::{Deletion, DeletionStore, Erasure, RetentionPeriods};
use crate::domain::erasure::ErasureQueue;
use crate::domain::error::DomainError;
use crate::domain::subject_settings::SubjectKey;
use crate::infra::panics::panic_message;

/// How many erasures wait for a worker on one instance; more are left to
/// the leader's job.
const QUEUE: usize = 1024;

/// How old a mark is before the leader's job runs its erasure. A run the
/// request started stops at this age, so the two never run it at once.
pub const STALE: Duration = Duration::from_mins(5);

// A request's own run waits out its mark, then removes the facts once more,
// which takes as long as a write; it has that much time before it stops.
const _: () = assert!(STALE.as_secs() >= 2 * crate::domain::deletion::ERASURE_WAIT.as_secs());

/// How many stopped erasures one run of the job takes.
const BATCH: u64 = 100;

/// The shortest and the longest pause before the leader election is tried
/// again.
const RETRY_FIRST: Duration = Duration::from_secs(5);
const RETRY_LONGEST: Duration = Duration::from_mins(5);

/// Log how one erasure run ended. A failed run leaves the mark, so the
/// leader's job runs it again. A store that did not answer is a warning; a
/// missing processing identity, said once at start, is debug; a failure a
/// rerun does not cure, such as a defect of the stored profile or a refusal,
/// is logged as an error, as the erasure stays stuck until someone fixes it.
#[expect(
    clippy::cognitive_complexity,
    reason = "six log lines; the tracing macros count toward the complexity"
)]
fn log_erasure(key: SubjectKey, ended: Result<Erasure, DomainError>) {
    match ended {
        Ok(Erasure::Done) => tracing::info!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            "construct: erasure done"
        ),
        Ok(Erasure::NotUnderWay) => tracing::debug!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            "construct: no erasure under way"
        ),
        // Said once at start: without a processing identity no erasure runs.
        Err(DomainError::NotConfigured(_)) => tracing::debug!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            "construct: the erasure waits for a processing identity"
        ),
        Ok(Erasure::LeftToLeader) => tracing::debug!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            "construct: the erasure is old enough for the leader's job to run"
        ),
        Err(err @ (DomainError::Unavailable(_) | DomainError::Database(_))) => tracing::warn!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            error = %err,
            "construct: an erasure stopped; it stays marked and the leader runs it again"
        ),
        Err(err) => tracing::error!(
            tenant_id = %key.tenant_id,
            subject_id = %key.subject_id,
            error = %err,
            "construct: an erasure failed in a way a rerun does not cure; it stays marked and \
             the leader keeps trying"
        ),
    }
}

/// Log how one tenant's retention sweep ended. Without a processing identity
/// every sweep ends so; the gear says it once at start.
#[expect(
    clippy::cognitive_complexity,
    reason = "three log lines; the tracing macros count toward the complexity"
)]
fn log_retention(
    tenant_id: Uuid,
    ended: Result<Result<(), DomainError>, Box<dyn std::any::Any + Send>>,
) {
    match ended {
        Ok(Ok(())) => {}
        Ok(Err(DomainError::NotConfigured(_))) => tracing::debug!(
            %tenant_id,
            "construct: retention waits for a processing identity"
        ),
        Ok(Err(err)) => tracing::warn!(
            %tenant_id,
            error = %err,
            "construct: retention stopped for the tenant; the next run goes on"
        ),
        Err(panic) => tracing::error!(
            %tenant_id,
            panic = panic_message(panic.as_ref()),
            "construct: retention panicked for the tenant; the next run goes on"
        ),
    }
}

/// [`ErasureQueue`] as a bounded channel to this instance's
/// [`ErasureWorker`].
///
/// @cpt-dod:cpt-cf-construct-dod-erasure-run:p1
pub struct ChannelErasures {
    sender: mpsc::Sender<SubjectKey>,
}

#[cfg(test)]
impl ChannelErasures {
    /// Whether the worker ended and let go of its end of the queue.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl ErasureQueue for ChannelErasures {
    fn enqueue(&self, key: SubjectKey) {
        // @cpt-begin:cpt-cf-construct-flow-erasure-request:p1:inst-request-hand-off
        if let Err(err) = self.sender.try_send(key) {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                subject_id = %key.subject_id,
                error = %err,
                "construct: the erasure is left to the leader's job"
            );
        }
        // @cpt-end:cpt-cf-construct-flow-erasure-request:p1:inst-request-hand-off
    }
}

/// Runs the erasures this instance took: their graph and table steps at
/// most `workers` at once, while any number wait out their marks.
pub struct ErasureWorker {
    receiver: mpsc::Receiver<SubjectKey>,
    deletion: Arc<Deletion>,
    busy: Arc<Semaphore>,
}

/// A queue for the erasure requests and the worker that drains it.
#[must_use]
pub fn erasure_queue(
    deletion: Arc<Deletion>,
    workers: NonZeroUsize,
) -> (ChannelErasures, ErasureWorker) {
    let (sender, receiver) = mpsc::channel(QUEUE);
    (
        ChannelErasures { sender },
        ErasureWorker {
            receiver,
            deletion,
            busy: Arc::new(Semaphore::new(workers.get())),
        },
    )
}

/// The erasures one worker is running, by task.
#[derive(Default)]
struct Running {
    tasks: JoinSet<()>,
    keys: HashMap<Id, SubjectKey>,
}

impl Running {
    fn holds(&self, key: SubjectKey) -> bool {
        self.keys.values().any(|running| *running == key)
    }

    /// Note that a task ended; log it when it panicked, with its subject.
    fn ended(&mut self, joined: Result<(Id, ()), JoinError>) {
        match joined {
            Ok((id, ())) => {
                self.keys.remove(&id);
            }
            Err(err) => {
                let key = self.keys.remove(&err.id());
                if !err.is_cancelled() {
                    tracing::error!(
                        tenant_id = key.map(|key| key.tenant_id.to_string()),
                        subject_id = key.map(|key| key.subject_id.to_string()),
                        error = %err,
                        "construct: an erasure panicked; it stays marked and the leader runs it \
                         again"
                    );
                }
            }
        }
    }
}

impl ErasureWorker {
    /// Run queued erasures until `cancel` fires: up to [`QUEUE`] at once,
    /// whose graph and table steps take turns for the workers' permits. An
    /// erasure this worker is running already is not started twice, and one
    /// marked longer than [`STALE`] ago is left to the leader's job.
    ///
    /// NOT cancel-safe for the erasures in flight: they stop where they are,
    /// with their mark set, and the leader's job runs them again.
    pub async fn run(mut self, cancel: CancellationToken) {
        let mut running = Running::default();
        loop {
            // Room for one more first, then the next erasure.
            if running.tasks.len() >= QUEUE {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    Some(joined) = running.tasks.join_next_with_id() => running.ended(joined),
                }
                continue;
            }
            let key = tokio::select! {
                () = cancel.cancelled() => return,
                Some(joined) = running.tasks.join_next_with_id(), if !running.tasks.is_empty() => {
                    running.ended(joined);
                    continue;
                }
                key = self.receiver.recv() => key,
            };
            let Some(key) = key else {
                return;
            };
            if running.holds(key) {
                continue;
            }
            let (deletion, busy) = (Arc::clone(&self.deletion), Arc::clone(&self.busy));
            let task = running.tasks.spawn(async move {
                log_erasure(key, deletion.erase_requested(key, STALE, &busy).await);
            });
            running.keys.insert(task.id(), key);
        }
    }
}

/// The leader's job: the erasures that stopped, and retention.
///
/// @cpt-dod:cpt-cf-construct-dod-retention-job:p2
pub struct DeletionJob {
    pub deletion: Arc<Deletion>,
    pub store: Arc<dyn DeletionStore>,
    pub periods: Arc<dyn RetentionPeriods>,
    /// How often the stopped erasures are run again.
    pub interval: Duration,
    /// How often retention runs; periods are counted in days, so far less
    /// often than the reruns.
    pub retention_interval: Duration,
}

impl DeletionJob {
    /// One run: every erasure marked longer than [`STALE`] ago and still
    /// under way, up to [`BATCH`], then each tenant with a retention period.
    /// What fails is logged and left for the next run.
    pub async fn run_once(&self) {
        self.run_stopped_erasures().await;
        self.run_retention().await;
    }

    /// Every erasure marked longer than [`STALE`] ago, up to [`BATCH`], the
    /// longest untried first. Each attempt is noted before it runs, so an
    /// erasure that keeps failing goes behind the others and does not hold
    /// up the batch.
    async fn run_stopped_erasures(&self) {
        // @cpt-begin:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-run
        let marked_before = OffsetDateTime::now_utc() - STALE;
        match self.store.under_way(marked_before, BATCH).await {
            Ok(keys) => {
                for key in keys {
                    self.rerun(key).await;
                }
            }
            Err(err) => tracing::warn!(
                error = %err,
                "construct: the stopped erasures could not be listed; the next run tries again"
            ),
        }
        // @cpt-end:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-run
    }

    /// Note the attempt, then run the erasure.
    async fn rerun(&self, key: SubjectKey) {
        // @cpt-begin:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-attempt
        if let Err(err) = self.store.attempted(key, OffsetDateTime::now_utc()).await {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                subject_id = %key.subject_id,
                error = %err,
                "construct: the erasure's attempt could not be noted; it runs anyway"
            );
        }
        // @cpt-end:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-attempt
        // A panic ends this erasure's run only; the job goes on to the next.
        match AssertUnwindSafe(self.deletion.erase(key))
            .catch_unwind()
            .await
        {
            Ok(ended) => log_erasure(key, ended),
            Err(panic) => tracing::error!(
                tenant_id = %key.tenant_id,
                subject_id = %key.subject_id,
                panic = panic_message(panic.as_ref()),
                "construct: an erasure panicked; it stays marked and the leader runs it again"
            ),
        }
    }

    /// Retention, tenant by tenant, for each tenant with a period.
    async fn run_retention(&self) {
        // @cpt-begin:cpt-cf-construct-algo-retention-expire:p2:inst-expire-tenants
        for tenant_id in self.tenants().await {
            match self.periods.period(tenant_id).await {
                // A panic ends this tenant's sweep only; the job goes on to
                // the next tenant.
                Ok(Some(period)) => log_retention(
                    tenant_id,
                    AssertUnwindSafe(self.deletion.expire(tenant_id, period))
                        .catch_unwind()
                        .await,
                ),
                Ok(None) => {}
                Err(err) => tracing::warn!(
                    %tenant_id,
                    error = %err,
                    "construct: the tenant's retention period could not be read"
                ),
            }
        }
        // @cpt-end:cpt-cf-construct-algo-retention-expire:p2:inst-expire-tenants
    }

    /// The tenants given a period of their own and, with a default period,
    /// every tenant Construct holds a row for.
    async fn tenants(&self) -> BTreeSet<Uuid> {
        let mut tenants = self.periods.listed();
        if self.periods.has_default() {
            match self.store.tenants().await {
                Ok(held) => tenants.extend(held),
                Err(err) => tracing::warn!(
                    error = %err,
                    "construct: the tenants could not be listed; only the listed ones are swept"
                ),
            }
        }
        tenants
    }

    /// Run the job until `cancel` fires: the reruns of stopped erasures every
    /// `interval` and retention every `retention_interval`, each in a loop of
    /// its own, so a long retention sweep does not hold off a stopped
    /// erasure.
    ///
    /// NOT cancel-safe for the runs in flight: they stop where they are, and
    /// the next leader's runs go on from what is left.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tokio::join!(
            self.every_interval(&cancel, self.interval, || self.run_stopped_erasures()),
            self.every_interval(&cancel, self.retention_interval, || self.run_retention()),
        );
    }

    /// Run `step` at once, then `interval` after each run ends, until
    /// `cancel` fires.
    async fn every_interval<F, Fut>(&self, cancel: &CancellationToken, interval: Duration, step: F)
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        loop {
            // A panic outside one erasure or one tenant, such as in a listing,
            // ends this run only; the next comes after the interval.
            tokio::select! {
                () = cancel.cancelled() => return,
                ran = AssertUnwindSafe(step()).catch_unwind() => {
                    if let Err(panic) = ran {
                        tracing::error!(
                            panic = panic_message(panic.as_ref()),
                            "construct: a run of the deletion job panicked; the next run goes on"
                        );
                    }
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
        }
    }
}

/// Runs work on one instance of the cluster at a time.
#[async_trait]
pub trait Leadership: Send + Sync {
    /// Run `job` while this instance leads, until `cancel` fires.
    async fn lead(&self, job: Arc<DeletionJob>, cancel: CancellationToken);
}

/// The scope of Construct's elections on the cluster gear.
const SCOPE: &str = "construct";
/// The election of the deletion job.
const ELECTION: &str = "deletion";
/// How long a job that lost the lead may take to stop.
const STOP: Duration = Duration::from_secs(10);

/// The cluster profile Construct's leader election resolves.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConstructProfile;

impl cluster_sdk::ClusterProfile for ConstructProfile {
    const NAME: &'static str = "construct";
}

/// [`Leadership`] on the platform cluster gear's leader election, with the
/// `construct` profile. A deployment without it runs no job, and says so.
pub struct ClusterLeadership {
    hub: Arc<ClientHub>,
}

/// The pause before the next try of the leader election: doubling from
/// [`RETRY_FIRST`] up to [`RETRY_LONGEST`], with up to as much again at
/// random, so instances that failed together do not come back together.
struct Backoff(Duration);

impl Backoff {
    fn new() -> Self {
        Self(RETRY_FIRST)
    }

    fn next(&mut self) -> Duration {
        let pause = self.0;
        self.0 = self.0.saturating_mul(2).min(RETRY_LONGEST);
        pause.saturating_add(jitter(pause))
    }
}

/// Sleep for the next pause of `backoff`; false when `cancel` fires first.
async fn pause(backoff: &mut Backoff, cancel: &CancellationToken) -> bool {
    tokio::select! {
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(backoff.next()) => true,
    }
}

impl ClusterLeadership {
    #[must_use]
    pub fn new(hub: Arc<ClientHub>) -> Self {
        Self { hub }
    }

    /// The election, resolved once the cluster gear answers; none when
    /// `cancel` fires first.
    async fn election(&self, cancel: &CancellationToken) -> Option<LeaderElectionV1> {
        let mut backoff = Backoff::new();
        loop {
            let resolving = LeaderElectionV1::resolver(&self.hub)
                .profile(ConstructProfile)
                .require(LeaderElectionCapability::Linearizable)
                .resolve();
            let resolved = tokio::select! {
                () = cancel.cancelled() => return None,
                resolved = resolving => resolved.and_then(|election| election.scoped(SCOPE)),
            };
            match resolved {
                Ok(election) => return Some(election),
                Err(err) => tracing::warn!(
                    error = %err,
                    "construct: no leader election for the deletion job; stopped erasures are \
                     not run again and nothing is removed for retention until there is one"
                ),
            }
            if !pause(&mut backoff, cancel).await {
                return None;
            }
        }
    }
}

#[async_trait]
impl Leadership for ClusterLeadership {
    async fn lead(&self, job: Arc<DeletionJob>, cancel: CancellationToken) {
        // @cpt-begin:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-leader
        let mut backoff = Backoff::new();
        loop {
            let Some(election) = self.election(&cancel).await else {
                return;
            };
            let watch = tokio::select! {
                () = cancel.cancelled() => return,
                watch = election.elect(ELECTION) => watch,
            };
            match watch {
                Ok(watch) => {
                    backoff = Backoff::new();
                    let job = Arc::clone(&job);
                    // Dropping the watch on shutdown stops the job it runs.
                    // The job starts when this instance gains the lead and is
                    // cancelled when it loses it.
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        () = watch.run_while_leader(STOP, move |token| {
                            let job = Arc::clone(&job);
                            async move {
                                tracing::info!("construct: this instance leads the deletion job");
                                job.run(token).await;
                                tracing::info!(
                                    "construct: this instance no longer runs the deletion job"
                                );
                            }
                        }) => {}
                    }
                }
                Err(err) => tracing::warn!(
                    error = %err,
                    "construct: the deletion job's election could not be joined"
                ),
            }
            if !pause(&mut backoff, &cancel).await {
                return;
            }
        }
        // @cpt-end:cpt-cf-construct-algo-erasure-rerun:p1:inst-rerun-leader
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use serde_json::json;

    use super::*;
    use crate::config::RetentionConfig;
    use crate::domain::deletion::{DeletionPorts, ERASURE_WAIT, SubjectsPage};
    use crate::domain::profile::Category;
    use crate::infra::retention_periods::ConfigRetentionPeriods;
    use crate::test_support::{
        FixedDirectory, MemoryDeletionStore, MemoryProfile, MemoryWriter, ScriptedRoots,
        profile_writes, stored_fact,
    };

    struct Job {
        key: SubjectKey,
        profile: Arc<MemoryProfile>,
        store: Arc<MemoryDeletionStore>,
        roots: Arc<ScriptedRoots>,
        deletion: Arc<Deletion>,
    }

    /// A subject with one fact stored at second 10, listed in its tenant.
    fn job() -> Job {
        let key = SubjectKey {
            tenant_id: Uuid::new_v4(),
            subject_id: Uuid::new_v4(),
        };
        let profile = MemoryProfile::with(vec![stored_fact(
            Category::Skill,
            "skills",
            json!({ "name": "Rust" }),
            10,
        )]);
        let store = Arc::new(MemoryDeletionStore::default());
        let roots = Arc::new(ScriptedRoots::default());
        let deletion = Arc::new(Deletion::new(DeletionPorts {
            writes: Arc::new(profile_writes(
                Arc::clone(&profile),
                MemoryWriter::new(Arc::clone(&profile), 0),
                Some(Uuid::new_v4()),
            )),
            roots: Arc::clone(&roots) as _,
            directory: Arc::new(FixedDirectory {
                pages: vec![SubjectsPage {
                    subjects: vec![key.subject_id],
                    next: None,
                }],
                ..FixedDirectory::default()
            }),
            store: Arc::clone(&store) as _,
            reviews: Arc::default() as Arc<crate::test_support::MemoryReviews>,
            review_events: Arc::default() as Arc<crate::test_support::RecordingReviewEvents>,
            events: Arc::default() as Arc<crate::test_support::RecordingDeletionEvents>,
        }));
        Job {
            key,
            profile,
            store,
            roots,
            deletion,
        }
    }

    fn periods(retention: RetentionConfig) -> Arc<ConfigRetentionPeriods> {
        Arc::new(ConfigRetentionPeriods::new(retention))
    }

    #[tokio::test]
    async fn the_job_runs_the_stopped_erasures_but_leaves_a_fresh_one_to_its_request() {
        let job = job();
        let fresh = SubjectKey {
            tenant_id: Uuid::new_v4(),
            subject_id: Uuid::new_v4(),
        };
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - STALE * 2);
        job.store.mark_at(fresh, OffsetDateTime::now_utc());
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        assert_eq!(job.store.finished(), [job.key]);
        assert!(job.profile.live().is_empty());
        assert!(job.store.is_marked(fresh), "the request's own run has it");
    }

    #[tokio::test]
    async fn the_job_sweeps_the_listed_tenants_and_with_a_default_every_tenant_held() {
        let job = job();
        let held = Uuid::new_v4();
        job.store.tenants.lock().unwrap().insert(held);
        let days = NonZeroU32::new(30).unwrap();

        let listed_only = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: None,
                tenants: [(job.key.tenant_id, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };
        listed_only.run_once().await;
        let swept: Vec<Uuid> = job
            .store
            .expired
            .lock()
            .unwrap()
            .iter()
            .map(|(tenant, _)| *tenant)
            .collect();
        assert_eq!(swept, [job.key.tenant_id]);
        assert!(job.profile.live().is_empty(), "the old fact is removed");

        job.store.expired.lock().unwrap().clear();
        let with_default = DeletionJob {
            periods: periods(RetentionConfig {
                default_days: Some(days),
                tenants: [(job.key.tenant_id, days)].into(),
            }),
            ..listed_only
        };
        with_default.run_once().await;
        let swept: BTreeSet<Uuid> = job
            .store
            .expired
            .lock()
            .unwrap()
            .iter()
            .map(|(tenant, _)| *tenant)
            .collect();
        assert_eq!(swept, BTreeSet::from([job.key.tenant_id, held]));
    }

    #[tokio::test]
    async fn without_a_period_nothing_is_swept() {
        let job = job();
        job.store.tenants.lock().unwrap().insert(job.key.tenant_id);
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        assert!(job.store.expired.lock().unwrap().is_empty());
        assert_eq!(job.profile.live().len(), 1);
    }

    fn another_subject() -> SubjectKey {
        SubjectKey {
            tenant_id: Uuid::new_v4(),
            subject_id: Uuid::new_v4(),
        }
    }

    fn removed_roots(job: &Job) -> usize {
        job.roots.calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn the_worker_runs_no_more_graph_steps_at_once_than_it_has_workers_and_waits_without_one()
    {
        let job = job();
        let keys = [job.key, another_subject(), another_subject()];
        for key in keys {
            job.store.mark_at(key, OffsetDateTime::now_utc());
        }
        let step = Duration::from_secs(10);
        *job.roots.takes.lock().unwrap() = step;
        let (queue, worker) = erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::MIN);
        let cancel = CancellationToken::new();
        let running = tokio::spawn(worker.run(cancel.clone()));
        let started = tokio::time::Instant::now();

        for key in keys {
            queue.enqueue(key);
        }
        tokio::time::timeout(ERASURE_WAIT * 3, async {
            while job.store.finished().len() < keys.len() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("every erasure finishes");

        assert_eq!(
            job.roots
                .most_at_once
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one worker, so one graph step at a time"
        );
        // Each erasure looks at its root twice, one at a time; the waits
        // after the marks run side by side, not one after another.
        let waited = started.elapsed();
        assert!(
            waited < ERASURE_WAIT + step * 6 + Duration::from_secs(1),
            "{waited:?}"
        );
        cancel.cancel();
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_worker_does_not_start_an_erasure_it_is_running_already() {
        let job = job();
        job.store.mark_at(job.key, OffsetDateTime::now_utc());
        let (queue, worker) =
            erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::new(4).unwrap());
        let cancel = CancellationToken::new();
        let running = tokio::spawn(worker.run(cancel.clone()));

        queue.enqueue(job.key);
        tokio::time::sleep(Duration::from_secs(1)).await;
        queue.enqueue(job.key);
        tokio::time::sleep(ERASURE_WAIT * 2).await;

        assert_eq!(
            removed_roots(&job),
            1,
            "one run for both requests, which removes the root after its wait"
        );
        assert_eq!(job.store.finished(), [job.key]);
        cancel.cancel();
        running.await.unwrap();
    }

    /// Wait until `done` holds, for at most ten seconds.
    async fn until(what: &str, done: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    #[tokio::test]
    async fn the_worker_leaves_an_erasure_marked_long_ago_to_the_leader() {
        let job = job();
        let fresh = another_subject();
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - STALE * 2);
        job.store
            .mark_at(fresh, OffsetDateTime::now_utc() - ERASURE_WAIT);
        let (queue, worker) = erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::MIN);
        let cancel = CancellationToken::new();
        let running = tokio::spawn(worker.run(cancel.clone()));

        // One worker runs them in turn, so the fresh one finishing shows the
        // stale one's run ended.
        queue.enqueue(job.key);
        queue.enqueue(fresh);
        until("the fresh erasure runs", || {
            !job.store.finished().is_empty()
        })
        .await;
        cancel.cancel();
        running.await.unwrap();

        assert_eq!(job.store.finished(), [fresh]);
        assert_eq!(
            removed_roots(&job),
            1,
            "only the fresh one's root, after its wait"
        );
        assert!(job.store.is_marked(job.key), "the leader's job runs it");
    }

    #[tokio::test]
    async fn a_tenant_that_fails_does_not_stop_the_others() {
        let job = job();
        // The tenants are swept in order; this one sorts first.
        let other = Uuid::from_u128(1);
        *job.store.failing_tenant.lock().unwrap() = Some(other);
        let days = NonZeroU32::new(30).unwrap();
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: None,
                tenants: [(other, days), (job.key.tenant_id, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        let swept: Vec<Uuid> = job
            .store
            .expired
            .lock()
            .unwrap()
            .iter()
            .map(|(tenant, _)| *tenant)
            .collect();
        assert_eq!(swept, [job.key.tenant_id]);
        assert!(job.profile.live().is_empty());
    }

    #[tokio::test]
    async fn without_the_held_tenants_only_the_listed_ones_are_swept() {
        let job = job();
        job.store.tenants.lock().unwrap().insert(Uuid::new_v4());
        job.store
            .tenants_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let days = NonZeroU32::new(30).unwrap();
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: Some(days),
                tenants: [(job.key.tenant_id, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        let swept: Vec<Uuid> = job
            .store
            .expired
            .lock()
            .unwrap()
            .iter()
            .map(|(tenant, _)| *tenant)
            .collect();
        assert_eq!(swept, [job.key.tenant_id]);
    }

    #[test]
    fn the_election_backoff_doubles_up_to_its_longest_with_jitter() {
        let mut backoff = Backoff::new();
        let pauses: Vec<Duration> = (0..10).map(|_| backoff.next()).collect();
        assert!(pauses[0] >= RETRY_FIRST && pauses[0] <= RETRY_FIRST * 2);
        assert!(pauses[1] >= RETRY_FIRST * 2);
        assert!(
            pauses
                .iter()
                .all(|pause| *pause <= RETRY_LONGEST.saturating_mul(2))
        );
        assert!(pauses[9] >= RETRY_LONGEST, "it stays at the longest");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn an_erasure_that_panicked_can_run_again_on_the_same_worker() {
        let job = job();
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - ERASURE_WAIT);
        job.roots
            .panic_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (queue, worker) = erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::MIN);
        let cancel = CancellationToken::new();
        let running = tokio::spawn(worker.run(cancel.clone()));

        queue.enqueue(job.key);
        until("the panic is logged with its subject", || {
            logs_contain("an erasure panicked")
        })
        .await;
        assert!(logs_contain(&job.key.subject_id.to_string()));
        assert!(job.store.is_marked(job.key), "the panicked run left it");
        queue.enqueue(job.key);
        until("the subject is not stuck after the panic", || {
            !job.store.finished().is_empty()
        })
        .await;
        cancel.cancel();
        running.await.unwrap();
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn a_rerun_that_panics_is_logged_and_the_next_run_finishes_the_erasure() {
        let job = job();
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - STALE * 2);
        job.roots
            .panic_once
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        assert!(logs_contain("an erasure panicked"));
        assert!(logs_contain(&job.key.subject_id.to_string()));
        assert!(job.store.is_marked(job.key), "the panicked run left it");
        runner.run_once().await;
        assert_eq!(job.store.finished(), [job.key]);
    }

    /// A directory that panics listing one tenant and lists no subject in
    /// any other; it notes each tenant it is asked about.
    struct PanicsListing {
        tenant_id: Uuid,
        asked: std::sync::Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl crate::domain::deletion::ProfileDirectory for PanicsListing {
        async fn subjects(
            &self,
            ctx: &toolkit_security::SecurityContext,
            _cursor: Option<String>,
        ) -> Result<SubjectsPage, DomainError> {
            let tenant_id = ctx.subject_tenant_id();
            self.asked.lock().unwrap().push(tenant_id);
            assert_ne!(tenant_id, self.tenant_id, "listing this tenant panics");
            Ok(SubjectsPage {
                subjects: Vec::new(),
                next: None,
            })
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn a_tenant_whose_sweep_panics_is_logged_and_the_next_tenant_is_swept() {
        let job = job();
        let (first, second) = {
            let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
            (a.min(b), a.max(b))
        };
        let directory = Arc::new(PanicsListing {
            tenant_id: first,
            asked: std::sync::Mutex::default(),
        });
        let deletion = Arc::new(Deletion::new(DeletionPorts {
            writes: Arc::new(profile_writes(
                Arc::clone(&job.profile),
                MemoryWriter::new(Arc::clone(&job.profile), 0),
                Some(Uuid::new_v4()),
            )),
            roots: Arc::clone(&job.roots) as _,
            directory: Arc::clone(&directory) as _,
            store: Arc::clone(&job.store) as _,
            reviews: Arc::default() as Arc<crate::test_support::MemoryReviews>,
            review_events: Arc::default() as Arc<crate::test_support::RecordingReviewEvents>,
            events: Arc::default() as Arc<crate::test_support::RecordingDeletionEvents>,
        }));
        let days = NonZeroU32::new(30).unwrap();
        let runner = DeletionJob {
            deletion,
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: None,
                tenants: [(first, days), (second, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        assert_eq!(
            *directory.asked.lock().unwrap(),
            [first, second],
            "the tenants are swept in order, the second after the first panicked"
        );
        assert!(logs_contain("retention panicked for the tenant"));
        assert!(logs_contain(&first.to_string()));
        assert!(
            !logs_contain("a run of the deletion job panicked"),
            "the tenant's guard caught it, not the run's"
        );
    }

    /// Retention periods whose first read panics, then give no period.
    struct PanicsOnce {
        tenant_id: Uuid,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl RetentionPeriods for PanicsOnce {
        async fn period(&self, _tenant_id: Uuid) -> Result<Option<Duration>, DomainError> {
            let read = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(read > 0, "the first read of the period panics");
            Ok(None)
        }

        fn listed(&self) -> BTreeSet<Uuid> {
            [self.tenant_id].into()
        }

        fn has_default(&self) -> bool {
            false
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test(start_paused = true)]
    async fn a_run_that_panics_is_logged_and_the_job_runs_again_after_the_interval() {
        let job = job();
        let periods = Arc::new(PanicsOnce {
            tenant_id: job.key.tenant_id,
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: Arc::clone(&periods) as _,
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        });
        let reads = || periods.reads.load(std::sync::atomic::Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let running = tokio::spawn(Arc::clone(&runner).run(cancel.clone()));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(reads(), 1);
        assert!(logs_contain("a run of the deletion job panicked"));
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(reads(), 2, "the job is still running after the panic");

        cancel.cancel();
        running.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn the_worker_runs_no_more_erasures_at_once_than_its_queue_and_the_next_once_one_ends() {
        let job = job();
        let keys: Vec<SubjectKey> = (0..QUEUE).map(|_| another_subject()).collect();
        for key in &keys {
            job.store.mark_at(*key, OffsetDateTime::now_utc());
        }
        let (queue, worker) = erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::MIN);
        let cancel = CancellationToken::new();
        let running = tokio::spawn(worker.run(cancel.clone()));

        for key in &keys {
            queue.enqueue(*key);
        }
        let started = || job.profile.reads.lock().unwrap().len();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            started(),
            QUEUE,
            "each erasure read the profile once and waits out its mark"
        );
        let next = another_subject();
        job.store.mark_at(next, OffsetDateTime::now_utc());
        queue.enqueue(next);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(started(), QUEUE, "the next waits for room");

        tokio::time::sleep(ERASURE_WAIT * 2).await;
        assert!(
            job.store.finished().contains(&next),
            "it runs once one ended"
        );
        cancel.cancel();
        running.await.unwrap();
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn a_full_queue_drops_the_erasure_for_the_leader_without_waiting() {
        let job = job();
        let (queue, _worker) = erasure_queue(Arc::clone(&job.deletion), NonZeroUsize::MIN);

        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..=QUEUE {
                queue.enqueue(another_subject());
            }
        })
        .await
        .expect("enqueue never waits");

        assert!(logs_contain("the erasure is left to the leader's job"));
    }

    #[tokio::test(start_paused = true)]
    async fn the_job_runs_at_once_then_every_interval_until_cancelled() {
        let job = job();
        let days = NonZeroU32::new(30).unwrap();
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: None,
                tenants: [(job.key.tenant_id, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        });
        let runs = || job.store.expired.lock().unwrap().len();
        let cancel = CancellationToken::new();
        let running = tokio::spawn(Arc::clone(&runner).run(cancel.clone()));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(runs(), 1, "the first run starts at once");
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(runs(), 1, "the next waits for the interval");
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(runs(), 2);

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), running)
            .await
            .expect("the job ends when cancelled")
            .unwrap();
        assert_eq!(runs(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retention_runs_on_its_own_longer_interval() {
        let job = job();
        let days = NonZeroU32::new(30).unwrap();
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig {
                default_days: None,
                tenants: [(job.key.tenant_id, days)].into(),
            }),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(600),
        });
        let sweeps = || job.store.expired.lock().unwrap().len();
        let cancel = CancellationToken::new();
        let running = tokio::spawn(Arc::clone(&runner).run(cancel.clone()));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sweeps(), 1, "the first sweep starts at once");
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(sweeps(), 1, "not on the reruns' interval");
        tokio::time::sleep(Duration::from_secs(480)).await;
        assert_eq!(sweeps(), 2);

        cancel.cancel();
        running.await.unwrap();
    }

    /// Retention periods whose read never ends, for a sweep that hangs.
    struct Hanging(Uuid);

    #[async_trait]
    impl RetentionPeriods for Hanging {
        async fn period(&self, _tenant_id: Uuid) -> Result<Option<Duration>, DomainError> {
            std::future::pending().await
        }

        fn listed(&self) -> BTreeSet<Uuid> {
            [self.0].into()
        }

        fn has_default(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn a_retention_sweep_that_does_not_end_does_not_hold_off_a_stopped_erasure() {
        let job = job();
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - STALE * 2);
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: Arc::new(Hanging(job.key.tenant_id)),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        });
        let cancel = CancellationToken::new();
        let running = tokio::spawn(Arc::clone(&runner).run(cancel.clone()));

        until("the stopped erasure runs beside the sweep", || {
            job.store.finished() == [job.key]
        })
        .await;

        cancel.cancel();
        running.await.unwrap();
    }

    #[tracing_test::traced_test]
    #[tokio::test(start_paused = true)]
    async fn without_a_cluster_gear_the_election_is_tried_again_after_a_pause() {
        let job = job();
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        });
        let leadership = ClusterLeadership::new(Arc::new(ClientHub::new()));
        let cancel = CancellationToken::new();
        let tries = || {
            let tries = std::cell::Cell::new(0);
            logs_assert(|lines| {
                tries.set(
                    lines
                        .iter()
                        .filter(|line| line.contains("election could not be joined"))
                        .count(),
                );
                Ok(())
            });
            tries.get()
        };

        let leading = async {
            leadership.lead(runner, cancel.clone()).await;
        };
        let watching = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let first = tries();
            // The first pause is at most twice the shortest.
            tokio::time::sleep(RETRY_FIRST * 2 + Duration::from_secs(1)).await;
            let second = tries();
            cancel.cancel();
            (first, second)
        };
        let ((), (first, second)) = tokio::join!(leading, watching);

        assert_eq!(first, 1, "it is tried at once");
        assert!(second >= 2, "and again after the pause: {second}");
        assert!(job.store.expired.lock().unwrap().is_empty(), "no job ran");
    }

    /// A leader election whose watch the test drives.
    #[derive(Default)]
    struct DrivenElection {
        sender: std::sync::Mutex<Option<cluster_sdk::LeaderWatchSender>>,
    }

    impl DrivenElection {
        fn joined(&self) -> bool {
            self.sender.lock().unwrap().is_some()
        }
    }

    #[async_trait]
    impl cluster_sdk::LeaderElectionBackend for DrivenElection {
        fn features(&self) -> cluster_sdk::LeaderElectionFeatures {
            cluster_sdk::LeaderElectionFeatures::new(true)
        }

        async fn elect(
            &self,
            _name: &str,
        ) -> Result<cluster_sdk::LeaderWatch, cluster_sdk::ClusterError> {
            let (sender, _resign, watch) =
                cluster_sdk::LeaderWatch::channel(8, cluster_sdk::LeaderStatus::Follower);
            *self.sender.lock().unwrap() = Some(sender);
            Ok(watch)
        }

        async fn elect_with_config(
            &self,
            name: &str,
            _config: cluster_sdk::ElectionConfig,
        ) -> Result<cluster_sdk::LeaderWatch, cluster_sdk::ClusterError> {
            self.elect(name).await
        }
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn the_job_runs_while_this_instance_leads_and_stops_when_it_loses_the_lead() {
        let job = job();
        job.store
            .mark_at(job.key, OffsetDateTime::now_utc() - STALE * 2);
        let runner = Arc::new(DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        });
        let election = Arc::new(DrivenElection::default());
        let hub = ClientHub::new();
        cluster_sdk::test_support::StubClusterClient::for_profile("construct")
            .with_leader_election(Arc::clone(&election) as _)
            .register(&hub);
        let leadership = ClusterLeadership::new(Arc::new(hub));
        let cancel = CancellationToken::new();
        let leading = tokio::spawn({
            let cancel = cancel.clone();
            async move { leadership.lead(runner, cancel).await }
        });

        until("the election is joined", || election.joined()).await;
        assert!(job.store.is_marked(job.key), "a follower runs nothing");
        let sender = election.sender.lock().unwrap().take().unwrap();
        sender
            .send_status(cluster_sdk::LeaderStatus::Leader)
            .await
            .unwrap();
        until("the leader runs the stopped erasure", || {
            job.store.finished() == [job.key]
        })
        .await;
        sender
            .send_status(cluster_sdk::LeaderStatus::Lost)
            .await
            .unwrap();
        until("the job stops with the lead", || {
            logs_contain("this instance no longer runs the deletion job")
        })
        .await;

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), leading)
            .await
            .expect("the leadership ends on cancel")
            .unwrap();
        assert!(logs_contain("this instance leads the deletion job"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn an_erasure_that_keeps_failing_goes_behind_the_others_and_says_so() {
        let job = job();
        let (failing, done) = (job.key, another_subject());
        let started = OffsetDateTime::now_utc();
        job.store.mark_at(failing, started - STALE * 3);
        job.store.mark_at(done, started - STALE * 2);
        job.roots
            .answers
            .lock()
            .unwrap()
            .push_back(Err(DomainError::internal("a foreign root")));
        let runner = DeletionJob {
            deletion: Arc::clone(&job.deletion),
            store: Arc::clone(&job.store) as _,
            periods: periods(RetentionConfig::default()),
            interval: Duration::from_secs(60),
            retention_interval: Duration::from_secs(60),
        };

        runner.run_once().await;

        assert_eq!(job.store.finished(), [done], "the other one still ran");
        let attempted = job.store.attempts.lock().unwrap().clone();
        assert!(
            attempted
                .get(&(failing.tenant_id, failing.subject_id))
                .is_some_and(|at| *at >= started),
            "{attempted:?}"
        );
        assert!(logs_contain("a rerun does not cure"));

        // One marked after the failing one, but never tried, now runs first:
        // it takes the next failure, and the one tried before gets through.
        let later = another_subject();
        job.store.mark_at(later, started - STALE * 2);
        job.roots
            .answers
            .lock()
            .unwrap()
            .push_back(Err(DomainError::internal("a foreign root")));

        runner.run_once().await;

        assert_eq!(job.store.finished(), [done, failing]);
        assert!(job.store.is_marked(later));
    }
}
