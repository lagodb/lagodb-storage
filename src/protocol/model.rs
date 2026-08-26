use std::sync::Arc;

use crate::backend::{StorageProbeResult, StoreConfig};
use crate::error::{StorageError, StorageErrorKind, StorageResult};
use crate::handle::{FileHandle, OpenFlags};

/// Stable numeric representation of [`StorageErrorKind`] on the Unix protocol.
///
/// Keeping this type in the protocol layer prevents domain errors from acquiring a wire-level
/// numeric ABI. Additions require an explicit mapping in both directions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum WireErrorCode {
    InvalidPath = 1,
    NotFound = 2,
    Unsupported = 3,
    Protocol = 4,
    Backend = 5,
    Cache = 6,
    Io = 7,
    ClosedHandle = 8,
    Configuration = 9,
    ResourceExhausted = 10,
    Busy = 11,
    CacheFillAborted = 12,
    ExpiredCursor = 13,
    Conflict = 14,
    Ambiguous = 15,
}

impl WireErrorCode {
    pub(crate) fn from_kind(kind: StorageErrorKind) -> Self {
        match kind {
            StorageErrorKind::InvalidPath => Self::InvalidPath,
            StorageErrorKind::NotFound => Self::NotFound,
            StorageErrorKind::Unsupported => Self::Unsupported,
            StorageErrorKind::Protocol => Self::Protocol,
            StorageErrorKind::Backend => Self::Backend,
            StorageErrorKind::Cache => Self::Cache,
            StorageErrorKind::Io => Self::Io,
            StorageErrorKind::ClosedHandle => Self::ClosedHandle,
            StorageErrorKind::Configuration => Self::Configuration,
            StorageErrorKind::ResourceExhausted => Self::ResourceExhausted,
            StorageErrorKind::Busy => Self::Busy,
            StorageErrorKind::CacheFillAborted => Self::CacheFillAborted,
            StorageErrorKind::ExpiredCursor => Self::ExpiredCursor,
            StorageErrorKind::Conflict => Self::Conflict,
            StorageErrorKind::Ambiguous => Self::Ambiguous,
        }
    }

    pub(crate) fn kind(self) -> StorageErrorKind {
        match self {
            Self::InvalidPath => StorageErrorKind::InvalidPath,
            Self::NotFound => StorageErrorKind::NotFound,
            Self::Unsupported => StorageErrorKind::Unsupported,
            Self::Protocol => StorageErrorKind::Protocol,
            Self::Backend => StorageErrorKind::Backend,
            Self::Cache => StorageErrorKind::Cache,
            Self::Io => StorageErrorKind::Io,
            Self::ClosedHandle => StorageErrorKind::ClosedHandle,
            Self::Configuration => StorageErrorKind::Configuration,
            Self::ResourceExhausted => StorageErrorKind::ResourceExhausted,
            Self::Busy => StorageErrorKind::Busy,
            Self::CacheFillAborted => StorageErrorKind::CacheFillAborted,
            Self::ExpiredCursor => StorageErrorKind::ExpiredCursor,
            Self::Conflict => StorageErrorKind::Conflict,
            Self::Ambiguous => StorageErrorKind::Ambiguous,
        }
    }

    pub(crate) fn from_u16(code: u16) -> Option<Self> {
        match code {
            1 => Some(Self::InvalidPath),
            2 => Some(Self::NotFound),
            3 => Some(Self::Unsupported),
            4 => Some(Self::Protocol),
            5 => Some(Self::Backend),
            6 => Some(Self::Cache),
            7 => Some(Self::Io),
            8 => Some(Self::ClosedHandle),
            9 => Some(Self::Configuration),
            10 => Some(Self::ResourceExhausted),
            11 => Some(Self::Busy),
            12 => Some(Self::CacheFillAborted),
            13 => Some(Self::ExpiredCursor),
            14 => Some(Self::Conflict),
            15 => Some(Self::Ambiguous),
            _ => None,
        }
    }
}

/// Opaque server-issued cursor used to fetch the next page of a `List` request. The bytes inside
/// are server-private and the client must round-trip the value unchanged.
///
/// A `ListCursor` is bound to the server-side iterator that produced it; it has a
/// connection-local lifetime. Closing the socket releases the cursor and its backend stream.
///
/// The inner string is intentionally not exposed: a cursor must come from a server response,
/// never be hand-crafted by the client. The codec layer reaches the inner bytes through
/// [`crate::protocol::cursor_codec`] which is `pub(crate)` to the protocol module.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ListCursor(String);

impl ListCursor {
    /// Construct a cursor from the wire bytes. Crate-private so only the server-side issue path
    /// (in [`crate::service::list_session`]) and the codec can build cursors; clients can only
    /// receive them in a [`crate::protocol::WireResponsePayload::List`] response.
    pub(crate) fn from_wire(value: String) -> Self {
        Self(value)
    }

    /// Borrow the cursor bytes for codec use.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// One element of a `List` response: an object key under the attached context's bucket plus
/// the same `(size, etag)` facts that `head` would have surfaced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireListEntry {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified_ms: Option<i64>,
}

/// Client→server operation body after decode ([`crate::protocol::decode_request`]).
///
/// Staging is invisible on the wire: the database (caller) writes its own files into the staging
/// tree using [`crate::staging::StagingPathResolver`]. `Upload` is the only staging-related verb
/// the server exposes; it carries `(bucket, key)` instead of a [`FileHandle`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireRequestPayload {
    AttachManaged {
        volume_id: u64,
    },
    AttachConfigured {
        config: Arc<StoreConfig>,
    },
    Open {
        bucket: String,
        key: String,
        flags: OpenFlags,
    },
    Head {
        bucket: String,
        key: String,
    },
    Read {
        handle: FileHandle,
        offset: u64,
        len: u32,
    },
    Close {
        handle: FileHandle,
    },
    Upload {
        bucket: String,
        key: String,
    },
    /// Exercises the attached backend under `root_prefix` without involving cache or staging.
    ProbeStore {
        bucket: String,
        root_prefix: String,
    },
    InvalidateObjectCache {
        bucket: String,
        key: String,
    },
    /// Deletes a single object from the backend. Cache invalidation is an explicit caller action.
    Delete {
        bucket: String,
        key: String,
    },
    /// Deletes every object whose key starts with `prefix`. The prefix is required to be
    /// non-empty so a stray `""` does not become a "wipe the whole bucket" request.
    DeletePrefix {
        bucket: String,
        prefix: String,
    },
    /// Deletes one bounded set of keys using the backend bulk-delete path.
    DeleteObjects {
        bucket: String,
        keys: Vec<String>,
    },
    /// Lists objects in a bucket whose key starts with `prefix`. `cursor` is `None`
    /// for the first page; subsequent calls echo the `next_cursor` returned by the server.
    /// `page_size = 0` means "let the server pick a default".
    List {
        bucket: String,
        prefix: Option<String>,
        page_size: u32,
        cursor: Option<ListCursor>,
    },
    /// Releases a retained list cursor without draining the remaining stream.
    CloseList {
        cursor: ListCursor,
    },
}

/// Correlates multiplexed frames on one Unix connection (`request_id` echoes in responses).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireRequest {
    pub request_id: u64,
    pub payload: WireRequestPayload,
}

/// Server→client body; [`WireResponsePayload::Error`] maps a protocol error code and diagnostic
/// message back into [`StorageError`] at the client boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireResponsePayload {
    Attach {
        backend_identity: String,
    },
    Open {
        handle: FileHandle,
        size: u64,
        direct_io: bool,
    },
    Head {
        size: u64,
        etag: Option<String>,
    },
    Read {
        data: Vec<u8>,
        eof: bool,
    },
    Close,
    /// Reply to `Upload`: the upload size and backend etag (when the backend reported one). A
    /// successful `Upload` does **not** touch cache state; if a cached entry for the same key
    /// already exists, callers who want to observe the newly uploaded bytes must follow up with
    /// `InvalidateObjectCache`. The staging file is left on disk; the database (caller) owns
    /// staging-directory cleanup.
    Upload {
        size: u64,
        etag: Option<String>,
    },
    ProbeStore {
        result: StorageProbeResult,
    },
    InvalidateObjectCache {
        removed: bool,
    },
    /// Reply to `Delete`. Carries no fields: backends disagree on whether deleting a missing key
    /// is success or `NotFound`, so synthesising an `existed: bool` would mislead the caller.
    Delete,
    /// Reply to `DeletePrefix`: number of objects the backend acknowledged removing.
    DeletePrefix {
        deleted: u64,
    },
    DeleteObjects {
        deleted: u32,
    },
    /// Reply to `List`. `next_cursor = None` indicates the listing is complete; otherwise the
    /// client must echo the cursor on its next `List` call to fetch the next page.
    List {
        entries: Vec<WireListEntry>,
        next_cursor: Option<ListCursor>,
    },
    CloseList,
    Error {
        code: WireErrorCode,
        message: String,
    },
}

/// Paired with [`WireRequest::request_id`] for correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireResponse {
    pub request_id: u64,
    pub payload: WireResponsePayload,
}

impl WireResponse {
    pub fn error(request_id: u64, error: StorageError) -> Self {
        Self {
            request_id,
            payload: WireResponsePayload::Error {
                code: WireErrorCode::from_kind(error.kind()),
                message: error.diagnostic_message(),
            },
        }
    }

    pub fn into_result(self) -> StorageResult<WireResponsePayload> {
        match self.payload {
            WireResponsePayload::Error { code, message } => {
                Err(StorageError::from_snapshot(code.kind(), message))
            }
            payload => Ok(payload),
        }
    }
}
