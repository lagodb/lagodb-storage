//! Tests for the large-fill lease lifecycle (partial files, aborts, promotion).

use std::sync::Arc;
use std::time::Duration;

use crate::backend::MemoryObjectBackend;
use crate::backend::test_support::StoreRegistry;
use crate::cache::CacheIndex;
use crate::cache::CacheState;
use crate::error::{StorageError, StorageErrorKind};
use crate::handle::OpenFlags;
use crate::service::StorageService;
use crate::service::command::{
    CloseCommand, OpenCommand, ReadCommand, StorageCommand,
};

use super::fixtures::{
    BUCKET, BlockingRangeBackend, CountingCompleteIndex, DEFAULT_STORE, LARGE_KEY,
    close, default_location, invalidate_cmd, memory_cache, open_file, read,
    wait_until, wait_until_async, write_cache_file,
};

mod concurrent_fill;
mod fill_lifecycle;
mod promotion;
