//! Inbound request reader: owns the read half of the socket and produces decoded wire requests.
//!
//! The reader reuses an internal buffer across frames so consecutive `next_event` calls do not
//! each allocate a fresh `Vec`.

use tokio::net::unix::OwnedReadHalf;

use crate::error::StorageResult;
use crate::protocol::{WireRequest, decode_request};
use crate::transport::FrameReader;

/// Event produced by the inbound reader each loop iteration.
pub(super) enum InboundEvent {
    /// A full request frame was decoded. Boxed to keep the enum compact.
    ///
    /// READ requests are small and hot, so this does add one allocation on that path. Keep this
    /// shape until profiling shows small-READ QPS is allocator-bound; then consider a dedicated
    /// stack-carried READ event while keeping larger request payloads boxed.
    Request(Box<WireRequest>),
    /// The peer closed the read half cleanly.
    Closed,
}

pub(super) struct InboundReader {
    reader: FrameReader<OwnedReadHalf>,
}

impl InboundReader {
    pub(super) fn new(reader: OwnedReadHalf) -> Self {
        Self {
            reader: FrameReader::new(reader),
        }
    }

    pub(super) async fn next_event(&mut self) -> StorageResult<InboundEvent> {
        let Some(frame) = self.reader.read_frame().await? else {
            return Ok(InboundEvent::Closed);
        };
        Ok(InboundEvent::Request(Box::new(decode_request(frame)?)))
    }
}
