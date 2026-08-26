use std::future;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::{sleep, timeout};

use crate::backend::test_support::StoreRegistry;
use crate::backend::{MemoryObjectBackend, ObjectBackend};
use crate::cache::{CacheManager, InMemoryCacheIndex};
use crate::config::{StorageRuntime, StorageRuntimeConfig, StorageServerConfig};
use crate::error::StorageResult;
use crate::handle::OpenFlags;
use crate::object::{ObjectInfo, ObjectLocation, ObjectPath};
use crate::protocol::{
    WireRequest, WireRequestPayload, WireResponsePayload, decode_response,
    encode_request,
};
use crate::service::StorageService;
use crate::service::reply::ReadBody;
use crate::transport::{FrameReader, write_frame};

use super::dispatch::{StorageHandlerPayload, StorageHandlerResponse};
use super::pipeline::process_connection_with_shutdown;
use super::request_tasks::RequestTasks;
use super::response_budget::{QueuedResponse, ResponseByteLimiter};
use super::shutdown::ConnectionShutdown;
use super::writer::ResponseWriter;

static TEST_WIRE_ID: AtomicU64 = AtomicU64::new(0);
const TEST_STORE_ID: &str = "test-store";

mod lifecycle;
mod response_budget;

async fn send_request(stream: &mut UnixStream, request: WireRequest) {
    let frame = encode_request(&request).unwrap();
    write_frame(stream, &frame).await.unwrap();
}

async fn read_response(stream: &mut UnixStream) -> WireResponsePayload {
    let frame = read_frame(stream).await.unwrap().unwrap();
    decode_response(&frame).unwrap().into_result().unwrap()
}

async fn read_frame(stream: &mut UnixStream) -> StorageResult<Option<Vec<u8>>> {
    let mut reader = FrameReader::new(stream);
    Ok(reader.read_frame().await?.map(<[u8]>::to_vec))
}

struct HangingBackend {
    started: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[async_trait]
impl ObjectBackend for HangingBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        _range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let _guard = DropSignal {
            dropped: self.dropped.clone(),
        };
        future::pending::<StorageResult<bytes::Bytes>>().await
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("HangingBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("HangingBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("HangingBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("HangingBackend does not participate in delete_stream")
    }
}

struct DropSignal {
    dropped: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(dropped) =
            self.dropped.lock().expect("dropped mutex poisoned").take()
        {
            let _ = dropped.send(());
        }
    }
}

struct DelayedHeadBackend {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

struct DelayedRangeBackend {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl ObjectBackend for DelayedHeadBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let release = self.release.lock().expect("release mutex poisoned").take();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        Ok(bytes::Bytes::from(vec![
            0;
            (range.end - range.start) as usize
        ]))
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("DelayedHeadBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("DelayedHeadBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("DelayedHeadBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("DelayedHeadBackend does not participate in delete_stream")
    }
}

#[async_trait]
impl ObjectBackend for DelayedRangeBackend {
    async fn head(&self, _key: &ObjectPath) -> StorageResult<ObjectInfo> {
        Ok(ObjectInfo {
            size: 10,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        _key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        if let Some(started) =
            self.started.lock().expect("started mutex poisoned").take()
        {
            let _ = started.send(());
        }
        let release = self.release.lock().expect("release mutex poisoned").take();
        if let Some(release) = release {
            let _ = release.await;
        }
        Ok(bytes::Bytes::copy_from_slice(
            &b"abcdefghij"[range.start as usize..range.end as usize],
        ))
    }

    async fn put_from_file(
        &self,
        _key: &ObjectPath,
        _path: &std::path::Path,
        _len: u64,
    ) -> StorageResult<ObjectInfo> {
        unreachable!("DelayedRangeBackend does not participate in staging uploads")
    }

    fn list(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>>
    {
        unreachable!("DelayedRangeBackend does not participate in list")
    }

    async fn delete(&self, _key: &ObjectPath) -> StorageResult<()> {
        unreachable!("DelayedRangeBackend does not participate in delete")
    }

    fn delete_stream(
        &self,
        _bucket: &str,
        _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        unreachable!("DelayedRangeBackend does not participate in delete_stream")
    }
}

fn test_shutdown(drain_timeout: Duration) -> ConnectionShutdown {
    ConnectionShutdown { drain_timeout }
}

fn test_server_config(max_in_flight_requests: usize) -> StorageServerConfig {
    StorageServerConfig::default()
        .with_max_in_flight_requests(max_in_flight_requests)
        .without_response_write_timeout()
}

fn test_cache_dir() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = TEST_WIRE_ID.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("/tmp").join(format!(
        "lagodb-storage-wire-test-{}-{stamp}-{id}",
        std::process::id()
    ))
}
