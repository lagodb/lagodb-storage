//! Object cache, Unix streaming protocol, and a minimal storage service façade (`open`/`read`/…).

mod backend;
mod builder;
mod cache;
mod client;
mod config;
mod connection;
mod error;
mod handle;
mod object;
mod protocol;
mod request;
mod server;
mod service;
mod session;
mod staging;
mod transport;

pub use backend::{
    AzureStoreConfig, GcsStoreConfig, ManagedStoreRegistry, MemoryObjectBackend,
    ObjectBackend, S3CompatibleStoreConfig, S3Encryption, S3StoreConfig,
    SecretString, StorageProbeResult, StoreConfig,
};
pub use builder::StorageServerBuilder;
pub use cache::{
    AdmitSmallOutcome, CacheIndex, CacheState, CachedObjectMeta, InMemoryCacheIndex,
    LogicalCacheUsage, LruScanCursor, LruScanPage, MetaScanCursor, MetaScanPage,
    OpenHit, RedbCacheIndex, SmallCacheEntry, SmallScanCursor, SmallScanPage,
};
pub use client::{
    DEFAULT_CLIENT_CLEANUP_TIMEOUT, ExternalFdLease, ExternalFdPolicy, ListIter,
    ListSession, SeekFrom, SocketInterest, SocketWait, SocketWaitContext,
    StagingFile, StorageClient, StorageClientBuilder, StorageFile, UploadInfo,
};
pub use config::{
    CacheCleanupConfig, CacheRuntimeConfig, DEFAULT_CACHE_CLEANUP_BATCH_BYTES,
    DEFAULT_CACHE_CLEANUP_BATCH_ITEMS, DEFAULT_CACHE_CLEANUP_INTERVAL,
    DEFAULT_CACHE_CLEANUP_START_PERCENT, DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
    DEFAULT_CACHE_TOUCH_GRANULARITY, DEFAULT_CONNECTION_DRAIN_TIMEOUT,
    DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
    DEFAULT_MAX_PENDING_RESPONSE_BYTES, DEFAULT_MAX_PENDING_RESPONSES,
    DEFAULT_MAX_READ_SIZE, DEFAULT_RESPONSE_WRITE_TIMEOUT, RuntimeApplyReport,
    StorageRuntime, StorageRuntimeConfig, StorageServerConfig, StorageServiceConfig,
};
pub use error::{StorageError, StorageErrorKind, StorageResult};
pub use object::{
    BackendDataIdentity, DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT, ListEntry,
    ObjectInfo, ObjectLocation, ObjectPath,
};
pub use protocol::{MAX_BULK_DELETE_OBJECT_KEYS, MAX_LIST_PAGE_SIZE};
pub use request::{
    NoopRequestObserver, NoopRequestPolicy, OperationMeta, RequestContext,
    RequestHooks, RequestObserver, RequestOperation, RequestOutcome, RequestPolicy,
    RequestStatus, TracingRequestObserver,
};
pub use server::StorageServer;
pub use service::LIST_CURSOR_IDLE_TTL_MS;
pub use staging::StagingPathResolver;
pub use tokio_util::sync::CancellationToken;
