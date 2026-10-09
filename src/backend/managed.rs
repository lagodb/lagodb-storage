//! Runtime-owned managed volume slots and scoped backend refresh batches.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::{BackendPool, ConfiguredObjectBackend, ObjectBackend, StoreConfig};
use crate::error::{StorageError, StorageResult};
use crate::object::BackendDataIdentity;

pub(crate) struct ManagedStoreSlot {
    identity: BackendDataIdentity,
    backend: RwLock<Arc<dyn ObjectBackend>>,
}

impl ManagedStoreSlot {
    fn new(identity: BackendDataIdentity, backend: Arc<dyn ObjectBackend>) -> Self {
        Self {
            identity,
            backend: RwLock::new(backend),
        }
    }

    pub(crate) fn identity(&self) -> &BackendDataIdentity {
        &self.identity
    }

    pub(crate) fn backend(&self) -> Arc<dyn ObjectBackend> {
        self.backend
            .read()
            .expect("managed store slot rwlock poisoned; backend state is no longer trustworthy")
            .clone()
    }

    fn replace(&self, backend: Arc<dyn ObjectBackend>) {
        *self
            .backend
            .write()
            .expect("managed store slot rwlock poisoned; backend state is no longer trustworthy") = backend;
    }
}

#[derive(Clone, Default)]
pub struct ManagedStoreRegistry {
    slots: Arc<RwLock<HashMap<u64, Arc<ManagedStoreSlot>>>>,
    pool: Arc<BackendPool>,
}

impl ManagedStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn backend_pool(&self) -> &Arc<BackendPool> {
        &self.pool
    }

    pub(crate) fn resolve(
        &self,
        volume_id: u64,
    ) -> StorageResult<Arc<ManagedStoreSlot>> {
        self.slots
            .read()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy")
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("managed volume {volume_id}")))
    }

    /// Returns whether a managed volume is registered at the time of the query.
    pub fn contains(&self, volume_id: u64) -> bool {
        self.slots
            .read()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy")
            .contains_key(&volume_id)
    }

    /// Inserts a managed volume or updates its backend using the shared pool.
    ///
    /// Updating an existing volume requires the resolved physical identity to
    /// remain unchanged. A failed update leaves its current backend in place.
    pub async fn upsert_config(
        &self,
        volume_id: u64,
        config: StoreConfig,
    ) -> StorageResult<()> {
        let backend = self.pool.intern(Arc::new(config)).await?;
        let identity = BackendDataIdentity::from_config(backend.config());
        self.publish(volume_id, identity, backend)
    }

    /// Starts one reconcile's forced refresh batch.
    ///
    /// Reuse this batch for all volumes being refreshed by that reconcile.
    /// Separate batches do not deduplicate backend construction; the caller
    /// serializes reconciles for this registry.
    pub fn begin_refresh(&self) -> ManagedStoreRefresh<'_> {
        ManagedStoreRefresh {
            registry: self,
            backends: Vec::new(),
        }
    }

    fn publish(
        &self,
        volume_id: u64,
        identity: BackendDataIdentity,
        backend: Arc<ConfiguredObjectBackend>,
    ) -> StorageResult<()> {
        let backend: Arc<dyn ObjectBackend> = backend;
        let mut slots = self
            .slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy");
        if let Some(slot) = slots.get(&volume_id) {
            if slot.identity() != &identity {
                return Err(StorageError::conflict(format!(
                    "managed volume {volume_id} physical identity changed"
                )));
            }
            slot.replace(backend);
            return Ok(());
        }
        slots.insert(
            volume_id,
            Arc::new(ManagedStoreSlot::new(identity, backend)),
        );
        Ok(())
    }

    pub fn register_backend(
        &self,
        volume_id: u64,
        identity: BackendDataIdentity,
        backend: Arc<dyn ObjectBackend>,
    ) -> StorageResult<()> {
        let mut slots = self
            .slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy");
        if let Some(slot) = slots.get(&volume_id) {
            if slot.identity() != &identity {
                return Err(StorageError::conflict(format!(
                    "managed volume {volume_id} physical identity changed"
                )));
            }
            slot.replace(backend);
        } else {
            slots.insert(
                volume_id,
                Arc::new(ManagedStoreSlot::new(identity, backend)),
            );
        }
        Ok(())
    }

    pub fn remove(&self, volume_id: u64) -> bool {
        self.slots
            .write()
            .expect("managed store registry rwlock poisoned; volume state is no longer trustworthy")
            .remove(&volume_id)
            .is_some()
    }
}

/// A single reconcile's batch of newly constructed managed backends.
///
/// Equal requested configurations share one new backend within this batch.
/// Each volume is published independently, retaining its previous backend if
/// publication fails. Existing operations can keep their old backend `Arc`;
/// subsequent operations obtain the replacement from the managed slot.
///
/// Dropping the batch releases its temporary strong references. Successfully
/// published backends remain owned by their slots. This batch does not
/// deduplicate with other concurrent batches or update configured connections.
pub struct ManagedStoreRefresh<'a> {
    registry: &'a ManagedStoreRegistry,
    backends: Vec<Arc<ConfiguredObjectBackend>>,
}

impl ManagedStoreRefresh<'_> {
    /// Creates or reuses this batch's new backend and replaces one managed slot's
    /// backend reference. The previous backend is not modified in place.
    pub async fn refresh_config(
        &mut self,
        volume_id: u64,
        config: StoreConfig,
    ) -> StorageResult<()> {
        let backend = match self
            .backends
            .iter()
            .find(|backend| backend.requested_config() == &config)
        {
            Some(backend) => Arc::clone(backend),
            None => {
                let backend = self
                    .registry
                    .pool
                    .materialize_fresh(Arc::new(config))
                    .await?;
                // A volume's identity conflict must not discard the backend
                // prepared for other volumes with the same requested config.
                self.backends.push(Arc::clone(&backend));
                backend
            }
        };
        // Address resolution can fill in a region absent from the request.
        let identity = BackendDataIdentity::from_config(backend.config());
        self.registry.publish(volume_id, identity, backend)
    }
}

#[cfg(test)]
impl ManagedStoreRegistry {
    pub(crate) fn with_shared_backend<B: ObjectBackend + 'static>(
        self,
        name: &str,
        backend: Arc<B>,
    ) -> StorageResult<Self> {
        self.register_backend(1, BackendDataIdentity::memory(name), backend)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests;
