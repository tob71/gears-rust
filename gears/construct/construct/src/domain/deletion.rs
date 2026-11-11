//! Deletion: the erasure of everything Construct holds about a subject, and
//! the removal of what is past the tenant's retention period. One component
//! with two triggers that run the same delete code: facts are removed as
//! plans against the profile's version, read and written again on a
//! conflict, and Construct's own rows are removed from its tables.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, SemaphorePermit};
use toolkit_macros::domain_model;
use toolkit_security::{AccessScope, SecurityContext};
use uuid::Uuid;

use super::error::DomainError;
use super::plan::{NodeKey, Origin};
use super::planner::processing::Written;
use super::profile_read::StoredFact;
use super::profile_writes::{Attempt, ProfileWrites, WRITE_LANDED, WriteError, removals};
use super::record_intake::RECEIVE_WINDOW;
use super::review_queue::{
    OutsideChange, ReviewOutcome, ReviewQueueEvents, ReviewRequests, close_on_change,
};
use super::subject_settings::{SettingsWrite, SubjectKey};

/// Where each subject's profile generation is kept. The root key of a
/// subject's profile follows the generation, and an erasure moves it on.
#[async_trait]
pub trait ProfileGenerations: Send + Sync {
    /// The generation of the subject's profile; 0 for a subject never erased.
    async fn generation(&self, key: SubjectKey) -> Result<u64, DomainError>;
}

/// What the removal of a subject's profile root did.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootRemoval {
    /// The root is removed, with every fact it still listed for removal.
    Removed,
    /// The subject has no root in its current generation.
    Gone,
    /// This many live facts are still on the root: a write landed after the
    /// facts were removed. Nothing is removed.
    FactsLeft(usize),
}

/// Removes a subject's profile root once its facts are removed.
#[async_trait]
pub trait ProfileRoots: Send + Sync {
    /// Soft-delete the root of the subject's current generation, and every
    /// fact it lists for removal whose delete has not run.
    ///
    /// # Errors
    ///
    /// The errors of the profile store; nothing that failed is left half
    /// done for good, as removing again is safe.
    async fn remove_root(
        &self,
        ctx: &SecurityContext,
        key: SubjectKey,
    ) -> Result<RootRemoval, DomainError>;
}

/// One page of the subjects with a profile in a tenant.
#[domain_model]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubjectsPage {
    pub subjects: Vec<Uuid>,
    /// Where the next page starts; none after the last.
    pub next: Option<String>,
}

/// Lists the subjects that have a profile, for retention.
#[async_trait]
pub trait ProfileDirectory: Send + Sync {
    /// The page of subjects with a profile in the context's tenant that
    /// starts at `cursor`, or the first.
    async fn subjects(
        &self,
        ctx: &SecurityContext,
        cursor: Option<String>,
    ) -> Result<SubjectsPage, DomainError>;
}

/// How many rows of Construct's own tables retention removed in a tenant.
#[domain_model]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpiredRows {
    pub review_requests: u64,
    pub record_ids: u64,
}

/// Construct's own tables as erasure and retention change them.
#[async_trait]
pub trait DeletionStore: Send + Sync {
    /// Set personalization off and mark an erasure of the subject under way,
    /// marked `at`, creating the row when there is none; a mark already set
    /// keeps its time. Whether the scope covers the key is decided before
    /// anything is written, as for the subject's settings.
    async fn mark(
        &self,
        scope: &AccessScope,
        key: SubjectKey,
        at: OffsetDateTime,
    ) -> Result<SettingsWrite, DomainError>;

    /// When the erasure under way was marked; none when there is none.
    async fn marked(
        &self,
        scope: &AccessScope,
        key: SubjectKey,
    ) -> Result<Option<OffsetDateTime>, DomainError>;

    /// In one transaction: remove the subject's review requests and record
    /// identities, move its profile generation on and clear the mark and the
    /// last attempt. Only while the erasure marked at `marked_at`, as
    /// [`Self::marked`] gave it, is under way, so a run never finishes
    /// another erasure; whether it was.
    async fn finish(
        &self,
        scope: &AccessScope,
        key: SubjectKey,
        marked_at: OffsetDateTime,
    ) -> Result<bool, DomainError>;

    /// Note that the leader's job tried the subject's erasure `at`, while
    /// one is under way.
    async fn attempted(&self, key: SubjectKey, at: OffsetDateTime) -> Result<(), DomainError>;

    /// At most `limit` subjects, of any tenant, whose erasure was marked
    /// before `marked_before` and is still under way, by when they were
    /// last tried, or marked when never tried, the earliest first.
    async fn under_way(
        &self,
        marked_before: OffsetDateTime,
        limit: u64,
    ) -> Result<Vec<SubjectKey>, DomainError>;

    /// Remove the tenant's review requests created before `before` and its
    /// record identities received before it.
    async fn expire(
        &self,
        scope: &AccessScope,
        tenant_id: Uuid,
        before: OffsetDateTime,
    ) -> Result<ExpiredRows, DomainError>;

    /// The tenants Construct holds a row of its own for.
    async fn tenants(&self) -> Result<BTreeSet<Uuid>, DomainError>;
}

/// Where Construct notes the tenants it holds data for. Retention finds a
/// tenant through Construct's own rows, and an administrator's add writes
/// none: the add notes the tenant here, so its facts are swept too.
#[async_trait]
pub trait KnownTenants: Send + Sync {
    /// Note `tenant_id`; noting it again changes nothing.
    ///
    /// # Errors
    ///
    /// When the store cannot be reached.
    async fn note(&self, tenant_id: Uuid) -> Result<(), DomainError>;
}

/// The tenants' retention periods. The period is a tenant setting in the
/// settings service; until Construct declares it there, the deployment's
/// config gives it.
#[async_trait]
pub trait RetentionPeriods: Send + Sync {
    /// The tenant's retention period; none keeps data until it is deleted.
    async fn period(&self, tenant_id: Uuid) -> Result<Option<Duration>, DomainError>;

    /// The tenants given a period of their own.
    fn listed(&self) -> BTreeSet<Uuid>;

    /// Whether every other tenant has a period too.
    fn has_default(&self) -> bool;
}

/// What Deletion did, for the audit log. No event holds content.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeletionEvent {
    /// The subject asked to erase everything.
    ErasureRequested { tenant_id: Uuid, subject_id: Uuid },
    /// Every step of the subject's erasure is done.
    Erased { tenant_id: Uuid, subject_id: Uuid },
    /// Retention removed this many of the subject's facts.
    FactsExpired {
        tenant_id: Uuid,
        subject_id: Uuid,
        facts: usize,
    },
    /// Retention removed this many rows of the tenant's from Construct's
    /// tables.
    RowsExpired {
        tenant_id: Uuid,
        review_requests: u64,
        record_ids: u64,
    },
}

/// Where Deletion's events go: the structured log until the Audit gear
/// ships.
pub trait DeletionEvents: Send + Sync {
    fn record(&self, event: DeletionEvent);
}

/// How one run of an erasure ended.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Erasure {
    /// This run did the last step and cleared the mark.
    Done,
    /// No erasure of the subject is under way: none was asked for, or
    /// another run finished it.
    NotUnderWay,
    /// The erasure was marked so long ago that the leader's job runs it;
    /// this run did nothing.
    LeftToLeader,
}

/// How long after its mark an erasure waits before it removes the facts
/// again and then the record identities: as long as a write admitted before
/// the mark can still land, so no such write lands after the last removal.
/// A record whose settings were read before the mark has committed its
/// identity well before then.
pub const ERASURE_WAIT: Duration = WRITE_LANDED;

// A record that read the settings before the mark commits within its receive
// window; three of them leave room for its commit and for the clocks of two
// instances to differ.
const _: () = assert!(ERASURE_WAIT.as_secs() >= 3 * RECEIVE_WINDOW.as_secs());

/// The most pages of subjects one retention run walks in a tenant: far more
/// than a tenant holds, so it only stops a listing that never ends.
pub(crate) const MAX_PAGES: usize = 10_000;

/// How the subjects of one tenant's retention ended.
#[derive(Default)]
struct Swept {
    subjects: usize,
    /// Stopped before every fact past the period was removed.
    failed: usize,
    /// Every fact past the period removed, a review request left open.
    left_open: usize,
}

impl Swept {
    /// Count how one subject's retention ended. What failed is found again
    /// by the next run: a fact still past the period, a request still open
    /// on a fact gone.
    fn note(&mut self, subject_id: Uuid, ended: Result<Expired, DomainError>) {
        self.subjects += 1;
        match ended {
            Ok(Expired::All) => {}
            // Each close that failed is logged with its fact already.
            Ok(Expired::RequestsLeftOpen) => self.left_open += 1,
            Err(err) => {
                self.failed += 1;
                tracing::warn!(
                    %subject_id,
                    error = %err,
                    "construct: retention of a subject stopped; the next run goes on"
                );
            }
        }
    }

    /// One line for the tenant, so a sweep where every subject fails is not
    /// only a scatter of warnings. Only a subject whose removal stopped may
    /// still hold data past the period.
    fn report(&self) {
        let Self {
            subjects,
            failed,
            left_open,
        } = *self;
        if failed > 0 {
            tracing::error!(
                subjects,
                failed,
                left_open,
                "construct: retention left subjects of the tenant past its period"
            );
        } else if left_open > 0 {
            tracing::warn!(
                subjects,
                left_open,
                "construct: retention removed what was past the period, but left review \
                 requests open; the next run closes them"
            );
        }
    }
}

/// How retention of one subject ended when nothing stopped it.
enum Expired {
    All,
    /// The facts are removed; a review request on one stays open until the
    /// next run.
    RequestsLeftOpen,
}

/// A permit of `busy`, when there is one.
async fn permit(busy: Option<&Semaphore>) -> Result<Option<SemaphorePermit<'_>>, DomainError> {
    match busy {
        None => Ok(None),
        Some(busy) => busy
            .acquire()
            .await
            .map(Some)
            .map_err(|_| DomainError::internal("the erasure workers were closed")),
    }
}

/// What one removal of facts left: the keys of the facts it removed, and of
/// the subject's live facts after it.
struct Removal {
    removed: BTreeSet<NodeKey>,
    kept: BTreeSet<NodeKey>,
}

/// The time `period` before `now`, from which a tenant keeps its data; none
/// when it reaches before the earliest time there is, which keeps
/// everything. The profile read hides, and retention removes, what is older.
pub(crate) fn period_start(now: OffsetDateTime, period: Duration) -> Option<OffsetDateTime> {
    time::Duration::try_from(period)
        .ok()
        .and_then(|period| now.checked_sub(period))
}

/// The collaborators of Deletion.
#[domain_model]
pub struct DeletionPorts {
    pub writes: Arc<ProfileWrites>,
    pub roots: Arc<dyn ProfileRoots>,
    pub directory: Arc<dyn ProfileDirectory>,
    pub store: Arc<dyn DeletionStore>,
    pub reviews: Arc<dyn ReviewRequests>,
    pub review_events: Arc<dyn ReviewQueueEvents>,
    pub events: Arc<dyn DeletionEvents>,
}

/// Erasure and retention, run as Construct.
#[domain_model]
pub struct Deletion {
    ports: DeletionPorts,
}

impl Deletion {
    #[must_use]
    pub fn new(ports: DeletionPorts) -> Self {
        Self { ports }
    }

    /// Run the subject's erasure, when one is under way: remove its facts
    /// and its profile root from graph storage, wait until no record checked
    /// before the mark can still keep its identity, then remove its review
    /// requests and record identities, move its profile generation on and
    /// clear the mark, in one transaction. Every step can run again.
    ///
    /// NOT cancel-safe: a run dropped part way leaves the steps it did done
    /// and the mark set; the next run does the rest.
    ///
    /// # Errors
    ///
    /// Not configured without a processing identity; unavailable when the
    /// profile kept changing, a write may have landed, or a write landed
    /// after the facts were removed; otherwise the errors of the stores. The
    /// mark stays, so the erasure runs again.
    ///
    /// @cpt-dod:cpt-cf-construct-dod-erasure-run:p1
    #[tracing::instrument(skip_all, fields(tenant_id = %key.tenant_id, subject_id = %key.subject_id))]
    pub async fn erase(&self, key: SubjectKey) -> Result<Erasure, DomainError> {
        self.run(key, None).await
    }

    /// [`Self::erase`] for the run its request started: an erasure marked
    /// more than `stale` ago is left to the leader's job, and a run still
    /// going `stale` after the mark stops there, so the request's run and
    /// the leader's do not overlap on one instance. Runs can still overlap
    /// across instances, when a repeat request lands on another one or two
    /// instances both think they lead; that is safe, as every step can run
    /// again and the removals are against the profile's version.
    ///
    /// The graph and table steps run only while holding a permit of `busy`;
    /// the wait after the mark holds none, so it does not keep a worker.
    ///
    /// # Errors
    ///
    /// As [`Self::erase`].
    #[tracing::instrument(skip_all, fields(tenant_id = %key.tenant_id, subject_id = %key.subject_id))]
    pub async fn erase_requested(
        &self,
        key: SubjectKey,
        stale: Duration,
        busy: &Semaphore,
    ) -> Result<Erasure, DomainError> {
        self.run(key, Some((stale, busy))).await
    }

    async fn run(
        &self,
        key: SubjectKey,
        requested: Option<(Duration, &Semaphore)>,
    ) -> Result<Erasure, DomainError> {
        // @cpt-begin:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-marked
        let (construct, scope) = self.ports.writes.construct(key.tenant_id).await?;
        let Some(marked_at) = self.ports.store.marked(&scope, key).await? else {
            return Ok(Erasure::NotUnderWay);
        };
        let Some((stale, busy)) = requested else {
            return self.steps(&construct, &scope, key, marked_at, None).await;
        };
        let left = marked_at
            .checked_add(time::Duration::try_from(stale).unwrap_or(time::Duration::MAX))
            .map(|cut| cut - OffsetDateTime::now_utc())
            .and_then(|left| Duration::try_from(left).ok())
            .filter(|left| !left.is_zero());
        let Some(left) = left else {
            return Ok(Erasure::LeftToLeader);
        };
        // @cpt-end:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-marked
        let steps = self.steps(&construct, &scope, key, marked_at, Some(busy));
        if let Ok(done) = tokio::time::timeout(left, steps).await {
            return done;
        }
        tracing::warn!("construct: an erasure ran past its time; the leader's job runs it again");
        Ok(Erasure::LeftToLeader)
    }

    /// The steps of an erasure marked at `marked_at`, from the first removal
    /// to the finish, the graph and table steps each under a permit of
    /// `busy` when there is one.
    async fn steps(
        &self,
        construct: &SecurityContext,
        scope: &AccessScope,
        key: SubjectKey,
        marked_at: OffsetDateTime,
        busy: Option<&Semaphore>,
    ) -> Result<Erasure, DomainError> {
        {
            let _busy = permit(busy).await?;
            // The root stays until the wait is over: a write admitted before
            // the mark can still attach a fact to it, and the removal after
            // the wait finds that fact only through the root.
            self.remove_every_fact(construct, key).await?;
        }
        // @cpt-begin:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-wait
        let wait = marked_at
            .checked_add(time::Duration::try_from(ERASURE_WAIT).unwrap_or(time::Duration::MAX))
            .map(|ready| ready - OffsetDateTime::now_utc());
        if let Some(Ok(wait)) = wait.map(Duration::try_from) {
            tokio::time::sleep(wait).await;
        }
        // A plan that passed Admission before the mark can still have
        // landed during the wait, also creating a root where there was none;
        // it goes now, before the generation moves on and nothing looks at
        // this one again.
        let _busy = permit(busy).await?;
        // Another run may have finished this erasure during the wait, and the
        // subject started a new profile since: only the erasure this run
        // started is cleared and finished.
        if self.ports.store.marked(scope, key).await? != Some(marked_at) {
            return Ok(Erasure::NotUnderWay);
        }
        self.clear_graph(construct, key).await?;
        // @cpt-end:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-wait
        // @cpt-begin:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-finish
        if !self.ports.store.finish(scope, key, marked_at).await? {
            return Ok(Erasure::NotUnderWay);
        }
        self.ports.events.record(DeletionEvent::Erased {
            tenant_id: key.tenant_id,
            subject_id: key.subject_id,
        });
        Ok(Erasure::Done)
        // @cpt-end:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-finish
    }

    /// Remove every fact of the subject's current generation, then its root.
    async fn clear_graph(
        &self,
        construct: &SecurityContext,
        key: SubjectKey,
    ) -> Result<(), DomainError> {
        self.remove_every_fact(construct, key).await?;
        // @cpt-begin:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-root
        if let RootRemoval::FactsLeft(left) = self.ports.roots.remove_root(construct, key).await? {
            return Err(DomainError::Unavailable(format!(
                "{left} facts of subject {} were written after its facts were removed; the \
                 erasure runs again",
                key.subject_id
            )));
        }
        Ok(())
        // @cpt-end:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-root
    }

    /// Remove every live fact of the subject's current generation, keeping
    /// its root.
    async fn remove_every_fact(
        &self,
        construct: &SecurityContext,
        key: SubjectKey,
    ) -> Result<(), DomainError> {
        // @cpt-begin:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-facts
        let origin = Origin::Subject {
            subject: key.subject_id,
        };
        self.remove_facts(construct, key, &origin, "erasure", |_| true)
            .await?;
        Ok(())
        // @cpt-end:cpt-cf-construct-algo-erasure-erase:p1:inst-erase-facts
    }

    /// Remove what is past `period` in the tenant: its review requests and
    /// record identities first, then, subject by subject, the facts stored
    /// before the period began, closing an open review request on each as
    /// deleted. A subject that fails is logged and left for the next run.
    ///
    /// NOT cancel-safe: a run dropped part way leaves the removals it did
    /// done; the next run does the rest.
    ///
    /// # Errors
    ///
    /// Not configured without a processing identity; the errors of the
    /// stores for the tenant's rows and its list of subjects.
    ///
    /// @cpt-dod:cpt-cf-construct-dod-retention-job:p2
    #[tracing::instrument(skip_all, fields(%tenant_id))]
    pub async fn expire(&self, tenant_id: Uuid, period: Duration) -> Result<(), DomainError> {
        // @cpt-begin:cpt-cf-construct-algo-retention-expire:p2:inst-expire-rows
        let Some(before) = period_start(OffsetDateTime::now_utc(), period) else {
            return Ok(());
        };
        let (construct, scope) = self.ports.writes.construct(tenant_id).await?;
        let rows = self.ports.store.expire(&scope, tenant_id, before).await?;
        if rows != ExpiredRows::default() {
            self.ports.events.record(DeletionEvent::RowsExpired {
                tenant_id,
                review_requests: rows.review_requests,
                record_ids: rows.record_ids,
            });
        }
        // @cpt-end:cpt-cf-construct-algo-retention-expire:p2:inst-expire-rows
        // @cpt-begin:cpt-cf-construct-algo-retention-expire:p2:inst-expire-subjects
        // The tenant's line is logged also when the listing fails part way,
        // so the subjects that failed before it are counted.
        let mut swept = Swept::default();
        let listed = self
            .expire_subjects(&construct, &scope, tenant_id, before, &mut swept)
            .await;
        swept.report();
        listed
        // @cpt-end:cpt-cf-construct-algo-retention-expire:p2:inst-expire-subjects
    }

    /// Each subject of the tenant with a profile, a page at a time, through
    /// [`Self::expire_facts`]; what each subject's retention ended with is
    /// counted in `swept`.
    async fn expire_subjects(
        &self,
        construct: &SecurityContext,
        scope: &AccessScope,
        tenant_id: Uuid,
        before: OffsetDateTime,
        swept: &mut Swept,
    ) -> Result<(), DomainError> {
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let page = self
                .ports
                .directory
                .subjects(construct, cursor.clone())
                .await?;
            for subject_id in page.subjects {
                let key = SubjectKey {
                    tenant_id,
                    subject_id,
                };
                swept.note(
                    subject_id,
                    self.expire_facts(construct, scope, key, before).await,
                );
            }
            match page.next {
                None => {
                    cursor = None;
                    break;
                }
                // A listing that hands back the page it was asked for would
                // keep the job on this tenant for good.
                Some(next) if cursor.as_ref() == Some(&next) => {
                    return Err(DomainError::internal(
                        "graph storage listed the same page of profile roots again; retention of \
                         the tenant stops for this run",
                    ));
                }
                Some(next) => cursor = Some(next),
            }
        }
        // A listing still going at the page limit is a defect like the
        // repeated page: the rest of the tenant is not swept, so the run fails.
        match cursor {
            Some(_) => Err(DomainError::internal(format!(
                "retention stopped listing the tenant's subjects at its page limit of \
                 {MAX_PAGES}; the rest of the tenant is not swept in this run"
            ))),
            None => Ok(()),
        }
    }

    /// Remove the subject's facts stored before `before`, then close as
    /// deleted every open review request whose fact is gone: those of the
    /// facts removed now, and any an earlier run removed but did not close.
    /// Every close is tried; each that fails is logged with its fact.
    async fn expire_facts(
        &self,
        construct: &SecurityContext,
        scope: &AccessScope,
        key: SubjectKey,
        before: OffsetDateTime,
    ) -> Result<Expired, DomainError> {
        // @cpt-begin:cpt-cf-construct-algo-retention-expire:p2:inst-expire-facts
        let Removal { removed, kept } = self
            .remove_facts(construct, key, &Origin::Retention, "retention", |fact| {
                fact.stored_at < before
            })
            .await?;
        if !removed.is_empty() {
            self.ports.events.record(DeletionEvent::FactsExpired {
                tenant_id: key.tenant_id,
                subject_id: key.subject_id,
                facts: removed.len(),
            });
        }
        // @cpt-end:cpt-cf-construct-algo-retention-expire:p2:inst-expire-facts
        // @cpt-begin:cpt-cf-construct-algo-retention-expire:p2:inst-expire-close
        let gone: Vec<NodeKey> = self
            .ports
            .reviews
            .under_review(scope, key)
            .await?
            .into_iter()
            .filter(|fact_id| !kept.contains(fact_id))
            .collect();
        // Construct closes the request; it names Construct's own identity.
        let by = construct.subject_id();
        let mut expired = Expired::All;
        for fact_id in &gone {
            // A fact this run removed had no reviewer's write in the
            // profile; one gone before may have.
            let change = OutsideChange {
                by,
                outcome: ReviewOutcome::Deleted,
                changed: removed.contains(fact_id),
            };
            if let Err(err) = close_on_change(
                self.ports.reviews.as_ref(),
                self.ports.review_events.as_ref(),
                &self.ports.writes,
                scope,
                key,
                fact_id,
                change,
            )
            .await
            {
                tracing::warn!(
                    subject_id = %key.subject_id,
                    fact_id = %fact_id.0,
                    error = %err,
                    "construct: a review request on a fact retention removed stays open until \
                     the next run"
                );
                expired = Expired::RequestsLeftOpen;
            }
        }
        Ok(expired)
        // @cpt-end:cpt-cf-construct-algo-retention-expire:p2:inst-expire-close
    }

    /// Remove every live fact of the subject that `chosen` picks, as one plan
    /// against the profile's version: read and written again after a short
    /// pause on a conflict, within the rerun limit and the write budget, so
    /// a fact a write changed in between is decided again.
    async fn remove_facts(
        &self,
        construct: &SecurityContext,
        key: SubjectKey,
        origin: &Origin,
        what: &str,
        chosen: impl Fn(&StoredFact) -> bool,
    ) -> Result<Removal, DomainError> {
        let writes = &self.ports.writes;
        let deadline = writes.deadline();
        let chosen = &chosen;
        writes
            .with_reruns(deadline, what, move || async move {
                let current =
                    ProfileWrites::before_write(deadline, what, writes.read(construct, key))
                        .await?;
                let (gone, kept): (Vec<StoredFact>, Vec<StoredFact>) =
                    current.facts.into_iter().partition(|fact| chosen(fact));
                let kept: BTreeSet<NodeKey> = kept.into_iter().map(|fact| fact.node_key).collect();
                if gone.is_empty() {
                    return Ok(Attempt::Done(Removal {
                        removed: BTreeSet::new(),
                        kept,
                    }));
                }
                let ids: BTreeSet<&NodeKey> = gone.iter().map(|fact| &fact.node_key).collect();
                let plan = removals(key, current.version, &gone, origin.clone());
                let written = writes
                    .write(construct, &plan, |now| {
                        !now.facts.iter().any(|fact| ids.contains(&fact.node_key))
                    })
                    .await?;
                Ok(match written {
                    Written::Stored => Attempt::Done(Removal {
                        removed: gone.into_iter().map(|fact| fact.node_key).collect(),
                        kept,
                    }),
                    Written::Conflict => Attempt::Conflict,
                })
            })
            .await
            .map_err(|err| match err {
                // Only a write that did not answer may have landed; any other
                // failure, such as a refusal, is passed on as it is.
                WriteError::MaybeWritten(DomainError::Unavailable(err)) => {
                    DomainError::Unavailable(format!(
                        "the {what} of subject {}'s facts may have landed; it runs again: {err}",
                        key.subject_id
                    ))
                }
                WriteError::NotWritten(err) | WriteError::MaybeWritten(err) => err,
            })
    }
}
