//! Weak interning pool for configured object-storage backends.

use std::sync::{Arc, Mutex, Weak};

use super::{ConfiguredObjectBackend, StoreConfig};
use crate::error::StorageResult;

/// Shares live backends without extending their lifetime. No pool lock is held
/// while AWS resolves its region or configures its credential provider.
#[derive(Default)]
pub struct BackendPool {
    entries: Mutex<Vec<Weak<ConfiguredObjectBackend>>>,
}

impl BackendPool {
    pub async fn intern(
        &self,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Arc<ConfiguredObjectBackend>> {
        config.validate()?;
        if let Some(backend) = self.find(&config) {
            return Ok(backend);
        }
        let backend = Arc::new(ConfiguredObjectBackend::new(config).await);
        let mut entries = self.entries.lock().expect("backend pool mutex poisoned");
        // Another attach can complete region resolution while ours is in flight.
        if let Some(existing) = entries
            .iter()
            .filter_map(Weak::upgrade)
            .find(|entry| entry.requested_config() == backend.requested_config())
        {
            return Ok(existing);
        }
        entries.push(Arc::downgrade(&backend));
        Ok(backend)
    }

    fn find(&self, config: &StoreConfig) -> Option<Arc<ConfiguredObjectBackend>> {
        let mut entries = self.entries.lock().expect("backend pool mutex poisoned");
        let mut matching = None;
        entries.retain(|entry| {
            let Some(backend) = entry.upgrade() else {
                return false;
            };
            if matching.is_none() && backend.requested_config() == config {
                matching = Some(backend);
            }
            true
        });
        matching
    }

    /// Resolves a new address even when the serialized configuration is unchanged.
    pub(super) async fn materialize_fresh(
        &self,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Arc<ConfiguredObjectBackend>> {
        config.validate()?;
        let backend = Arc::new(ConfiguredObjectBackend::new(config).await);
        let mut entries = self.entries.lock().expect("backend pool mutex poisoned");
        entries.retain(|entry| {
            entry.upgrade().is_some_and(|entry| {
                entry.requested_config() != backend.requested_config()
            })
        });
        entries.push(Arc::downgrade(&backend));
        Ok(backend)
    }
}
