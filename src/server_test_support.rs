use std::path::Path;
use std::sync::Arc;

use crate::cache::CacheIndex;
use crate::config::StorageServerConfig;
use crate::error::StorageResult;
use crate::request::RequestHooks;
use crate::service::StorageService;

use super::StorageServer;

impl<I: CacheIndex + 'static> StorageServer<I> {
    pub(crate) async fn bind(
        socket_path: impl AsRef<Path>,
        service: Arc<StorageService<I>>,
    ) -> StorageResult<Self> {
        Self::bind_with_config(socket_path, service, StorageServerConfig::default())
            .await
    }

    pub(crate) async fn bind_with_config(
        socket_path: impl AsRef<Path>,
        service: Arc<StorageService<I>>,
        config: StorageServerConfig,
    ) -> StorageResult<Self> {
        Self::bind_with_config_and_hooks(
            socket_path,
            service,
            config,
            RequestHooks::default(),
        )
        .await
    }
}
