use std::sync::Arc;

use crate::backend::ObjectBackend;
use crate::cache::Residency;
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFileState};
use crate::object::{ObjectInfo, ObjectLocation};

use super::{HandleTable, ReservedOpen};

impl HandleTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Opens a handle with a synthetic residency for tests that do not exercise OPEN.
    pub(crate) fn open(
        &self,
        key: ObjectLocation,
        backend: Arc<dyn ObjectBackend>,
        info: ObjectInfo,
    ) -> StorageResult<OpenFileState> {
        let slot = self.reserve_open()?;
        let residency = Arc::new(Residency::for_test(key.clone(), info.clone()));
        Ok(self.open_reserved(ReservedOpen {
            slot,
            key,
            backend,
            info,
            residency,
        }))
    }

    pub(crate) fn get(&self, handle: FileHandle) -> StorageResult<OpenFileState> {
        self.lock_handles()
            .get(&handle.0)
            .map(|entry| entry.state.clone())
            .ok_or_else(|| StorageError::closed_handle(handle.0))
    }
}
