use std::env;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::ManagedStoreRegistry;
use crate::cache::{CacheIndex, CacheManager};
use crate::config::{DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION, StorageServiceConfig};
use crate::request::RequestHooks;
use crate::session::{AttachedStorageContext, StorageContext};
use crate::staging::StagingUploader;

use super::StorageService;

static TEST_STAGING_ROOT_ID: AtomicU64 = AtomicU64::new(0);

fn test_staging_root() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let id = TEST_STAGING_ROOT_ID.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "lagodb-storage-staging-test-{}-{stamp}-{id}",
        process::id()
    ))
}

impl<I: CacheIndex> StorageService<I> {
    pub(crate) fn test_context(&self) -> StorageContext<I> {
        self.test_context_with_handle_limit(DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION)
    }

    pub(crate) fn test_context_with_handle_limit(
        &self,
        max_open_handles: usize,
    ) -> StorageContext<I> {
        let service = Arc::new(Self {
            managed_stores: self.managed_stores.clone(),
            backend_pool: Arc::clone(&self.backend_pool),
            cache: Arc::clone(&self.cache),
            staging_uploader: Arc::clone(&self.staging_uploader),
            config: self.config.clone(),
        });
        let slot = service
            .managed_stores
            .resolve(1)
            .expect("test managed volume must be registered");
        let context = StorageContext::new_with_hooks_and_handle_limit(
            "test",
            service,
            RequestHooks::default(),
            max_open_handles,
        );
        context
            .attach(AttachedStorageContext::Managed(slot))
            .expect("test context is initially unattached");
        context
    }

    pub(crate) fn with_registry(
        managed_stores: ManagedStoreRegistry,
        cache: Arc<CacheManager<I>>,
    ) -> Self {
        Self::with_registry_config(
            managed_stores,
            cache,
            StorageServiceConfig::default(),
        )
    }

    pub(crate) fn with_registry_config(
        managed_stores: ManagedStoreRegistry,
        cache: Arc<CacheManager<I>>,
        config: StorageServiceConfig,
    ) -> Self {
        Self::with_staging_uploader(
            managed_stores,
            cache,
            Arc::new(StagingUploader::new(test_staging_root())),
            config,
        )
    }
}
