use std::sync::Arc;

use crate::cache::CacheIndex;
use crate::config::DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION;
use crate::request::RequestHooks;
use crate::service::StorageService;

use super::StorageContext;

impl<I: CacheIndex> StorageContext<I> {
    pub(crate) fn new_with_hooks(
        client_addr: impl Into<Arc<str>>,
        service: Arc<StorageService<I>>,
        request_hooks: RequestHooks,
    ) -> Self {
        Self::new_with_hooks_and_handle_limit(
            client_addr,
            service,
            request_hooks,
            DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
        )
    }
}
