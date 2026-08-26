//! Unix connection framing: `u32` big-endian length prefix then payload bytes.
//!
//! Direct-I/O `OPEN` responses write the ordinary frame first, then send a one-byte
//! [`SCM_RIGHTS`](libc::SCM_RIGHTS) token through [`FdSender`] and receives it through
//! [`try_recv_fd`] so frame ordering stays independent from ancillary FD delivery.
//!
//! The server uses [`FrameReader`], [`write_frame`], and [`FdSender`]. The synchronous client uses
//! [`BlockingFrameCursor`], the blocking framing functions, and [`try_recv_fd`] through its
//! runtime-aware socket adapter. Ancillary-data unsafe code remains confined to [`fd_channel`].

mod bind;
mod blocking;
mod fd_channel;
mod frame;

pub(crate) use bind::bind_storage_unix_listener;
pub(crate) use blocking::BlockingFrameCursor;
pub(crate) use blocking::{read_frame_blocking, write_frame_blocking};
pub(crate) use fd_channel::FdSender;
pub(crate) use fd_channel::try_recv_fd;
pub(crate) use frame::{FrameReader, write_frame};
