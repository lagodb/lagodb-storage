use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{
    ManagedStoreRegistry, MemoryObjectBackend, ObjectBackend,
    S3CompatibleStoreConfig, SecretString, StoreConfig,
};
use crate::cache::{CacheIndex, CacheManager, CachedObjectMeta, InMemoryCacheIndex};
use crate::config::CacheCleanupPolicy;
use crate::config::{StorageRuntime, StorageRuntimeConfig, StorageServerConfig};
use crate::error::StorageErrorKind;
use crate::object::{BackendDataIdentity, ObjectInfo, ObjectLocation};
use crate::protocol::{
    WireRequestPayload, WireResponse, WireResponsePayload, decode_request,
    encode_response,
};
use crate::server::StorageServer;
use crate::service::StorageService;
use crate::staging::{StagingPathResolver, StagingUploader};
use crate::transport::{read_frame_blocking, write_frame_blocking};

use super::*;

const TEST_STORE_ID: &str = "test-store";
const TEST_VOLUME_ID: u64 = 1;

fn test_identity() -> BackendDataIdentity {
    BackendDataIdentity::memory(TEST_STORE_ID)
}

fn test_root(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    PathBuf::from("/tmp").join(format!("lagodb-storage-client-{name}-{stamp}"))
}

trait TestManagedRegistryExt {
    fn register_shared_backend<B: ObjectBackend + 'static>(
        &self,
        _name: &str,
        backend: Arc<B>,
    ) -> StorageResult<()>;
}

impl TestManagedRegistryExt for ManagedStoreRegistry {
    fn register_shared_backend<B: ObjectBackend + 'static>(
        &self,
        _name: &str,
        backend: Arc<B>,
    ) -> StorageResult<()> {
        self.register_backend(TEST_VOLUME_ID, test_identity(), backend)
    }
}

type StoreRegistry = ManagedStoreRegistry;

#[derive(Clone)]
struct CountingFdPolicy {
    active: Arc<AtomicUsize>,
    limit: Option<usize>,
}

impl ExternalFdPolicy for CountingFdPolicy {
    fn acquire(&self) -> StorageResult<Box<dyn ExternalFdLease>> {
        if let Some(limit) = self.limit {
            self.active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |active| {
                    (active < limit).then_some(active + 1)
                })
                .map_err(|_| {
                    StorageError::resource_exhausted(
                        "test external file descriptor budget exhausted",
                    )
                })?;
        } else {
            self.active.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Box::new(CountingFdLease {
            active: Arc::clone(&self.active),
        }))
    }
}

struct CountingFdLease {
    active: Arc<AtomicUsize>,
}

impl ExternalFdLease for CountingFdLease {}

impl Drop for CountingFdLease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

mod lifecycle;
mod positioned_reads;
mod protocol_ops;
mod server_io;
mod staging;
