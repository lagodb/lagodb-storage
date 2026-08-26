//! Async length-prefixed framing.
//!
//! [`FrameReader`] owns the reusable receive buffer. [`write_frame`] is stateless because
//! response buffers are already encoded before they reach the transport layer.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{StorageError, StorageResult};
use crate::protocol::MAX_FRAME_BYTES;

/// Stateful reader for length-prefixed frames.
///
/// The returned frame borrows the reader's internal buffer, which is reused by the next call.
/// Callers must decode or copy the frame before reading another one.
pub(crate) struct FrameReader<R> {
    reader: R,
    frame: Vec<u8>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            frame: Vec::new(),
        }
    }

    /// Reads one frame and returns `Ok(None)` on EOF between frames.
    pub(crate) async fn read_frame(&mut self) -> StorageResult<Option<&[u8]>> {
        let mut len_buf = [0_u8; 4];
        let mut prefix_read = 0;
        while prefix_read < len_buf.len() {
            match self.reader.read(&mut len_buf[prefix_read..]).await {
                Ok(0) if prefix_read == 0 => return Ok(None),
                Ok(0) => {
                    return Err(StorageError::protocol(
                        "truncated frame length prefix",
                    ));
                }
                Ok(read) => prefix_read += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    return Err(StorageError::io("read frame length prefix", error));
                }
            }
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(StorageError::protocol(format!("frame too large: {len}")));
        }
        self.frame.resize(len, 0);
        match self.reader.read_exact(&mut self.frame).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(StorageError::protocol_source(
                    "truncated frame payload",
                    error,
                ));
            }
            Err(error) => {
                return Err(StorageError::io("read frame payload", error));
            }
        }
        Ok(Some(&self.frame))
    }

    pub(crate) fn into_inner(self) -> R {
        self.reader
    }
}

/// Writes one length-prefixed frame and flushes.
///
/// Small frames (≤ 8 KiB) are coalesced with their 4-byte length prefix into a single
/// `write_all` to save a syscall on unbuffered writers. Larger frames use two separate
/// writes to avoid allocating and copying up to [`MAX_FRAME_BYTES`] (64 MiB) of data.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> StorageResult<()> {
    let len = frame.len();
    if len > MAX_FRAME_BYTES {
        return Err(StorageError::protocol(format!("frame too large: {len}")));
    }
    let len_buf = (len as u32).to_be_bytes();
    if len <= 8192 {
        let mut buf = Vec::with_capacity(4 + len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(frame);
        writer.write_all(&buf).await?;
    } else {
        writer.write_all(&len_buf).await?;
        writer.write_all(frame).await?;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt as _, duplex};

    use super::*;

    #[tokio::test]
    async fn clean_eof_between_frames_returns_none() {
        let (mut peer, stream) = duplex(8);
        peer.shutdown().await.unwrap();

        let mut reader = FrameReader::new(stream);
        assert!(reader.read_frame().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn partial_length_prefix_is_not_clean_eof() {
        let (mut peer, stream) = duplex(8);
        peer.write_all(&[0, 0]).await.unwrap();
        peer.shutdown().await.unwrap();

        let mut reader = FrameReader::new(stream);
        let error = reader.read_frame().await.unwrap_err();
        match error {
            StorageError::Protocol { context, .. } => {
                assert_eq!(context, "truncated frame length prefix");
            }
            error => panic!("expected protocol error, got {error:?}"),
        }
    }
}
