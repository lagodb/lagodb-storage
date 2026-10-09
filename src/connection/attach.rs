//! Mandatory single-context handshake executed before request multiplexing starts.

use std::sync::Arc;
use std::time::Instant;

use tokio::net::UnixStream;

use crate::cache::CacheIndex;
use crate::error::StorageResult;
use crate::protocol::{
    WireResponse, WireResponsePayload, decode_request, encode_response,
};
use crate::request::{RequestContext, RequestHooks, RequestOutcome};
use crate::service::StorageService;
use crate::session::AttachedStorageContext;
use crate::transport::{FrameReader, write_frame};

/// Terminal outcome of the mandatory attach handshake.
///
/// Only failures that have not already crossed an observability boundary are returned as
/// [`crate::error::StorageError`]. A rejected attach has already been delivered to the configured
/// request observer and sent to the peer, while `PeerClosed` is a clean EOF before the first
/// frame.
pub(crate) enum AttachOutcome {
    Attached(UnixStream, AttachedStorageContext),
    PeerClosed,
    Rejected,
}

pub(crate) async fn attach<I: CacheIndex>(
    stream: UnixStream,
    service: &Arc<StorageService<I>>,
    request_hooks: &RequestHooks,
    client_addr: Arc<str>,
) -> StorageResult<AttachOutcome> {
    let mut reader = FrameReader::new(stream);
    let request = match reader.read_frame().await? {
        Some(frame) => decode_request(frame)?,
        None => return Ok(AttachOutcome::PeerClosed),
    };
    let mut stream = reader.into_inner();
    let request_id = request.request_id;
    let request_context =
        RequestContext::new(request_id, client_addr, &request.payload);
    let started = Instant::now();
    request_hooks.observer().on_request_start(&request_context);
    let resolved = match request_hooks.policy().before_dispatch(&request_context) {
        Ok(()) => service.resolve_attach(request.payload).await,
        Err(error) => Err(error),
    };
    let attached = match resolved {
        Ok(attached) => attached,
        Err(error) => {
            request_hooks.observer().on_request_finish(
                &request_context,
                &RequestOutcome::error(error.kind(), started.elapsed()),
            );
            let response = WireResponse::error(request_id, error);
            write_frame(&mut stream, &encode_response(&response)?).await?;
            return Ok(AttachOutcome::Rejected);
        }
    };
    request_hooks.observer().on_request_finish(
        &request_context,
        &RequestOutcome::success(started.elapsed()),
    );
    let response = WireResponse {
        request_id,
        payload: WireResponsePayload::Attach {
            backend_identity: attached.identity().cache_key().to_owned(),
        },
    };
    write_frame(&mut stream, &encode_response(&response)?).await?;
    Ok(AttachOutcome::Attached(stream, attached))
}
