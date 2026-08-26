use std::path::PathBuf;
use std::sync::Arc;

use crate::config::CacheCleanupPolicy;
use crate::error::{StorageError, StorageResult};
use crate::object::{ObjectInfo, ObjectLocation};

use super::{
    CacheCleanupReport, CacheIndex, CacheManager, CachedObjectMeta, LargeFillSession,
    Residency, ResidencyBody,
    janitor::CacheJanitor,
    object_state::{CacheActivityKind, ObjectStateRegistry},
};

impl Residency {
    pub(crate) fn for_test(key: ObjectLocation, info: ObjectInfo) -> Self {
        let payload_len =
            usize::try_from(info.size).expect("test object size must fit in memory");
        let meta = CachedObjectMeta::small(key.clone(), info, payload_len as u64);
        let (states, _reaper_inbox) = ObjectStateRegistry::new();
        let state = states.get_or_create(&key);
        Self {
            _lease: state.activity_guard(CacheActivityKind::OpenLease),
            body: ResidencyBody::Small {
                meta,
                payload: vec![0; payload_len].into(),
            },
        }
    }

    pub(crate) fn large_fill_session(&self) -> Option<Arc<LargeFillSession>> {
        match &self.body {
            ResidencyBody::LargeFill { session } => Some(Arc::clone(session)),
            _ => None,
        }
    }
}

impl<I: CacheIndex> CacheManager<I> {
    pub(crate) fn index(&self) -> &I {
        &self.index
    }

    pub(crate) fn has_live_large_fill(&self, key: &ObjectLocation) -> bool {
        self.object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .is_some()
    }

    pub(crate) fn live_large_fill_partial_path(
        &self,
        key: &ObjectLocation,
    ) -> Option<PathBuf> {
        self.object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .map(|session| session.partial_path().to_path_buf())
    }

    /// Runs the same janitor pass as the scheduler without waiting for its timer.
    pub(crate) async fn run_cleanup_for_test(
        &self,
        capacity: Option<CacheCleanupPolicy>,
    ) -> StorageResult<CacheCleanupReport> {
        CacheJanitor::new(self).cleanup(capacity).await
    }

    /// Bypasses the object lock so tests can race an abort with blocked backend I/O.
    pub(crate) async fn abort_live_large_fill_for_test(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()> {
        let session = self
            .object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .ok_or_else(|| {
                StorageError::cache(format!("no live large fill for {key}"))
            })?;
        self.abort_large_fill(&session).await
    }
}
