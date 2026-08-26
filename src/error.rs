//! Crate-wide [`StorageResult`], [`StorageError`], and domain error classification.

use std::error::Error;
use std::io;

pub type StorageResult<T> = Result<T, StorageError>;
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageErrorKind {
    InvalidPath,
    NotFound,
    Unsupported,
    Protocol,
    Backend,
    Cache,
    Io,
    ClosedHandle,
    Configuration,
    ResourceExhausted,
    Busy,
    CacheFillAborted,
    ExpiredCursor,
    Conflict,
    Ambiguous,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("invalid path: {message}")]
    InvalidPath { message: String },

    #[error("not found: {key}")]
    NotFound { key: String },

    #[error("unsupported operation: {operation}")]
    Unsupported { operation: String },

    #[error("protocol error: {context}")]
    Protocol {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    #[error("backend error: {context}")]
    Backend {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    #[error("cache error: {context}")]
    Cache {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    #[error("io error: {context}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },

    #[error("closed handle: {handle}")]
    ClosedHandle { handle: u64 },

    #[error("configuration error: {message}")]
    Configuration { message: String },

    #[error("resource exhausted: {message}")]
    ResourceExhausted { message: String },

    #[error("busy: {message}")]
    Busy { message: String },

    #[error("cache fill aborted: {message}")]
    CacheFillAborted { message: String },

    #[error("expired cursor: {message}")]
    ExpiredCursor { message: String },

    #[error("storage context conflict: {message}")]
    Conflict { message: String },

    #[error("ambiguous storage operation {operation}: {message}")]
    Ambiguous { operation: String, message: String },
}

impl StorageError {
    pub fn invalid_path(message: impl Into<String>) -> Self {
        Self::InvalidPath {
            message: message.into(),
        }
    }

    pub fn not_found(key: impl Into<String>) -> Self {
        Self::NotFound { key: key.into() }
    }

    pub fn unsupported(operation: impl Into<String>) -> Self {
        Self::Unsupported {
            operation: operation.into(),
        }
    }

    pub fn protocol(context: impl Into<String>) -> Self {
        Self::Protocol {
            context: context.into(),
            source: None,
        }
    }

    pub fn protocol_source(
        context: impl Into<String>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::Protocol {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn backend(context: impl Into<String>) -> Self {
        Self::Backend {
            context: context.into(),
            source: None,
        }
    }

    pub fn backend_source(
        context: impl Into<String>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::Backend {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn cache(context: impl Into<String>) -> Self {
        Self::Cache {
            context: context.into(),
            source: None,
        }
    }

    pub fn cache_source(
        context: impl Into<String>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::Cache {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn io(context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    /// Wraps a tokio [`JoinError`](tokio::task::JoinError) from a background task into an IO error.
    ///
    /// Join failures fall into two categories (panic and cancellation); both are surfaced as
    /// `StorageError::Io` because callers of this crate never interact with tokio's task model
    /// directly.
    pub(crate) fn from_join_error(
        context: &str,
        error: tokio::task::JoinError,
    ) -> Self {
        if error.is_cancelled() {
            Self::io(context, io::Error::other("cancelled"))
        } else {
            Self::io(context, io::Error::other(error))
        }
    }

    pub fn closed_handle(handle: u64) -> Self {
        Self::ClosedHandle { handle }
    }

    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            message: message.into(),
        }
    }

    pub fn busy(message: impl Into<String>) -> Self {
        Self::Busy {
            message: message.into(),
        }
    }

    pub fn cache_fill_aborted(key: impl std::fmt::Display) -> Self {
        Self::CacheFillAborted {
            message: format!(
                "large cache fill for {key} was aborted; close existing handles or explicitly invalidate the cache object before reopening"
            ),
        }
    }

    pub fn expired_cursor(message: impl Into<String>) -> Self {
        Self::ExpiredCursor {
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn ambiguous(
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Ambiguous {
            operation: operation.into(),
            message: message.into(),
        }
    }

    pub fn kind(&self) -> StorageErrorKind {
        match self {
            Self::InvalidPath { .. } => StorageErrorKind::InvalidPath,
            Self::NotFound { .. } => StorageErrorKind::NotFound,
            Self::Unsupported { .. } => StorageErrorKind::Unsupported,
            Self::Protocol { .. } => StorageErrorKind::Protocol,
            Self::Backend { .. } => StorageErrorKind::Backend,
            Self::Cache { .. } => StorageErrorKind::Cache,
            Self::Io { .. } => StorageErrorKind::Io,
            Self::ClosedHandle { .. } => StorageErrorKind::ClosedHandle,
            Self::Configuration { .. } => StorageErrorKind::Configuration,
            Self::ResourceExhausted { .. } => StorageErrorKind::ResourceExhausted,
            Self::Busy { .. } => StorageErrorKind::Busy,
            Self::CacheFillAborted { .. } => StorageErrorKind::CacheFillAborted,
            Self::ExpiredCursor { .. } => StorageErrorKind::ExpiredCursor,
            Self::Conflict { .. } => StorageErrorKind::Conflict,
            Self::Ambiguous { .. } => StorageErrorKind::Ambiguous,
        }
    }

    /// Lossy, source-flattened detail used when an error must cross an ownership boundary.
    ///
    /// Most callers should retain the structured error. Ownership and process boundaries use
    /// this representation when boxed error sources cannot cross that boundary, including the
    /// wire adapter and PostgreSQL error reporting in the host crate.
    pub fn diagnostic_message(&self) -> String {
        match self {
            Self::InvalidPath { message } => message.clone(),
            Self::NotFound { key } => key.clone(),
            Self::Unsupported { operation } => operation.clone(),
            Self::Protocol { context, source }
            | Self::Backend { context, source }
            | Self::Cache { context, source } => {
                message_with_optional_source(context, source.as_deref())
            }
            Self::Io { context, source } => format!("{context}: {source}"),
            Self::ClosedHandle { handle } => handle.to_string(),
            Self::Configuration { message } => message.clone(),
            Self::ResourceExhausted { message }
            | Self::Busy { message }
            | Self::CacheFillAborted { message }
            | Self::ExpiredCursor { message } => message.clone(),
            Self::Conflict { message } => message.clone(),
            Self::Ambiguous { operation, message } => {
                format!("{operation}: {message}")
            }
        }
    }

    pub(crate) fn from_snapshot(kind: StorageErrorKind, message: String) -> Self {
        match kind {
            StorageErrorKind::InvalidPath => Self::invalid_path(message),
            StorageErrorKind::NotFound => Self::not_found(message),
            StorageErrorKind::Unsupported => Self::unsupported(message),
            StorageErrorKind::Protocol => Self::protocol(message),
            StorageErrorKind::Backend => Self::backend(message),
            StorageErrorKind::Cache => Self::cache(message),
            StorageErrorKind::Io => {
                Self::io("remote io error", io::Error::other(message))
            }
            StorageErrorKind::ClosedHandle => message
                .parse::<u64>()
                .map(Self::closed_handle)
                .unwrap_or_else(|_| {
                    Self::protocol(format!(
                        "invalid closed-handle error payload: {message:?}"
                    ))
                }),
            StorageErrorKind::Configuration => Self::configuration(message),
            StorageErrorKind::ResourceExhausted => Self::resource_exhausted(message),
            StorageErrorKind::Busy => Self::busy(message),
            StorageErrorKind::CacheFillAborted => Self::CacheFillAborted { message },
            StorageErrorKind::ExpiredCursor => Self::expired_cursor(message),
            StorageErrorKind::Conflict => Self::conflict(message),
            StorageErrorKind::Ambiguous => {
                Self::ambiguous("remote operation", message)
            }
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::io("io operation failed", value)
    }
}

fn message_with_optional_source(
    context: &str,
    source: Option<&(dyn Error + Send + Sync + 'static)>,
) -> String {
    match source {
        Some(source) => format!("{context}: {source}"),
        None => context.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn io_error_retains_source() {
        let error = StorageError::io(
            "open cache file",
            io::Error::other("permission denied"),
        );

        assert_eq!(error.to_string(), "io error: open cache file");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("permission denied")
        );
    }

    #[test]
    fn backend_error_retains_source() {
        let error = StorageError::backend_source(
            "head object bucket/file",
            io::Error::other("backend timed out"),
        );

        assert_eq!(error.to_string(), "backend error: head object bucket/file");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("backend timed out")
        );
    }

    #[test]
    fn snapshot_roundtrip_uses_detail_without_display_prefix() {
        let error = StorageError::invalid_path("missing bucket");
        let decoded =
            StorageError::from_snapshot(error.kind(), error.diagnostic_message());

        assert_eq!(decoded.to_string(), "invalid path: missing bucket");

        let error = StorageError::expired_cursor("unknown or expired list cursor");
        let decoded =
            StorageError::from_snapshot(error.kind(), error.diagnostic_message());

        assert_eq!(
            decoded.to_string(),
            "expired cursor: unknown or expired list cursor"
        );
    }
}
