//! Erasure: the subject's request to erase everything Construct holds about
//! it. The request marks the erasure and hands it to Deletion, which runs it
//! in the background; a run that stopped is run again by the leader's job.

use std::sync::Arc;

use authz_resolver_sdk::PolicyEnforcer;
use time::OffsetDateTime;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;
use uuid::Uuid;

use super::deletion::{DeletionEvent, DeletionEvents, DeletionStore};
use super::error::DomainError;
use super::profile_read::settings_scope;
use super::subject_access::own;
use super::subject_settings::{SettingsWrite, SubjectKey, SubjectSettings, out_of_scope};

pub(crate) mod actions {
    /// The subject erases everything Construct holds about it.
    pub const ERASE: &str = "erase";
}

/// Where a marked erasure goes to be run.
pub trait ErasureQueue: Send + Sync {
    /// Run the subject's erasure soon. A queue that cannot take it now drops
    /// it: the mark stays, and the leader's job runs it later.
    fn enqueue(&self, key: SubjectKey);
}

/// The subject's erasure request, owner only.
#[domain_model]
pub struct ErasureService {
    store: Arc<dyn DeletionStore>,
    queue: Arc<dyn ErasureQueue>,
    events: Arc<dyn DeletionEvents>,
    policy_enforcer: PolicyEnforcer,
}

impl ErasureService {
    #[must_use]
    pub fn new(
        store: Arc<dyn DeletionStore>,
        queue: Arc<dyn ErasureQueue>,
        events: Arc<dyn DeletionEvents>,
        policy_enforcer: PolicyEnforcer,
    ) -> Self {
        Self {
            store,
            queue,
            events,
            policy_enforcer,
        }
    }

    /// Erase everything about the subject, which is the caller: set its
    /// personalization off and mark the erasure under way, so nothing is
    /// served about it and no plan for it is stored from now on, then hand
    /// the erasure to Deletion. A request while one is under way marks
    /// nothing new and hands it on again. Returns the settings as they are
    /// now.
    ///
    /// cancel-safe: the mark is one write. A drop before it changes nothing;
    /// a drop after it leaves the erasure marked, and the leader's job runs
    /// it.
    ///
    /// # Errors
    ///
    /// [`DomainError::Forbidden`] when the caller is not the subject or the
    /// platform refuses; [`DomainError::OutOfScope`] when the scope does not
    /// reach the subject's settings; otherwise the errors of the store.
    ///
    /// @cpt-dod:cpt-cf-construct-dod-erasure-request:p1
    #[tracing::instrument(skip_all, fields(tenant_id = %ctx.subject_tenant_id(), %subject_id))]
    pub async fn request(
        &self,
        ctx: &SecurityContext,
        subject_id: Uuid,
    ) -> Result<SubjectSettings, DomainError> {
        // @cpt-begin:cpt-cf-construct-flow-erasure-request:p1:inst-request-own
        let (key, scope) = own(&self.policy_enforcer, ctx, subject_id, actions::ERASE).await?;
        // @cpt-end:cpt-cf-construct-flow-erasure-request:p1:inst-request-own
        // @cpt-begin:cpt-cf-construct-flow-erasure-request:p1:inst-request-mark
        match self
            .store
            .mark(&settings_scope(&scope), key, OffsetDateTime::now_utc())
            .await?
        {
            SettingsWrite::Written => {}
            SettingsWrite::OutOfScope(refusal) => {
                return Err(out_of_scope(ctx, key, refusal));
            }
        }
        // @cpt-end:cpt-cf-construct-flow-erasure-request:p1:inst-request-mark
        // @cpt-begin:cpt-cf-construct-flow-erasure-request:p1:inst-request-hand-off
        self.events.record(DeletionEvent::ErasureRequested {
            tenant_id: key.tenant_id,
            subject_id,
        });
        self.queue.enqueue(key);
        Ok(SubjectSettings {
            personalization_enabled: false,
            erasure_in_progress: true,
        })
        // @cpt-end:cpt-cf-construct-flow-erasure-request:p1:inst-request-hand-off
    }
}
