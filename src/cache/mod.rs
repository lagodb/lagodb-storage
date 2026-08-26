//! On-disk object cache: metadata index, large-file fills, small-object KV, eviction, and recovery.
//!
//! [`crate::builder::StorageServerBuilder`] owns production construction and lifecycle wiring;
//! [`CacheManager`] is its crate-internal cache orchestrator. Submodules are organized by concern:
//!
//! - [`index`]   — [`CacheIndex`] trait and implementations (in-memory, redb-persistent).
//! - [`path`]    — Deterministic on-disk path resolution for cache entries.
//! - [`store`]   — Physical cache store abstraction (file-based and small-object).
//! - [`manager`] — [`CacheManager`] struct, constructors, and core orchestration methods.
//! - `chunks`    — Large-object fill sessions (leader/follower, partial files, reaper).
//! - `admission` — Cache admission logic (open-hit, small-object, complete-file).
//! - `eviction`  — LRU-based capacity eviction and invalidation.
//! - `janitor`   — Periodic and on-demand cleanup coordination.
//! - `startup`   — Recovery and reconciliation at server boot.

mod admission;
mod chunks;
mod cleanup_scheduler;
mod establish;
mod eviction;
pub(crate) mod index;
mod inventory;
mod janitor;
pub(crate) mod manager;
mod meta;
mod object_state;
mod path;
mod startup;
mod store;
mod types;
mod usage;
mod util;

pub(crate) use chunks::{ChunkFillClaim, LargeFillSession};
pub(crate) use establish::EstablishLeader;
pub use index::{
    AdmitSmallOutcome, CacheIndex, InMemoryCacheIndex, LruScanCursor, LruScanPage,
    MetaScanCursor, MetaScanPage, OpenHit, RedbCacheIndex, SmallCacheEntry,
    SmallScanCursor, SmallScanPage,
};
pub(crate) use manager::CacheManager;
pub use meta::{CacheState, CachedObjectMeta};
pub(crate) use object_state::CacheActivityGuard;
pub(crate) use path::CachePathResolver;
pub(crate) use store::{
    DeleteReport, PhysicalCacheEntry, PhysicalCacheEntryVisitor, PhysicalCacheId,
    ScanControl,
};
pub(crate) use types::{
    CacheCleanupReport, CacheDeleteReason, CacheInvalidateReport, CacheRecoveryReport,
};
pub(crate) use types::{
    CacheEvictionOutcome, OpenOutcome, Residency, ResidencyBody, ResidencyStateHint,
};
pub use usage::LogicalCacheUsage;
pub(crate) use usage::PhysicalCacheUsage;
pub(crate) use util::{create_parent_dir, now_ns, should_touch};

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
