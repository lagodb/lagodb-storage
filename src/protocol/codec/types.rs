//! `WireEncode` / `WireDecode` for crate types reused inside wire frames.
//!
//! These types (`FileHandle`, `OpenFlags`, `SecretString`, store configs, errors) live outside the
//! protocol module so their wire layout is kept next to where it is used rather than on the types
//! themselves. Orphan rules permit this because every type is defined in this crate.

use bytes::{Buf, BufMut};

use crate::backend::SecretString;
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFlags};
use crate::protocol::model::{ListCursor, WireErrorCode, WireListEntry};

use super::traits::{WireDecode, WireEncode, get_u8, get_u16, get_u32, get_u64};

impl WireEncode for FileHandle {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u64(self.0);
        Ok(())
    }
}

impl WireDecode for FileHandle {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(FileHandle(get_u64(input)?))
    }
}

impl WireEncode for OpenFlags {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let bits = u8::from(self.read);
        out.put_u8(bits);
        Ok(())
    }
}

impl WireDecode for OpenFlags {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let bits = get_u8(input)?;
        if bits & !0b1 != 0 {
            return Err(StorageError::protocol(format!(
                "unknown open flags 0x{bits:02x}"
            )));
        }
        Ok(OpenFlags {
            read: bits & 1 != 0,
        })
    }
}

// `SecretString` serializes like a plain string on the wire; the type exists to keep credentials
// from leaking in debug output, not to change the layout.
impl WireEncode for SecretString {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self.expose_secret().as_bytes())
    }
}

impl WireDecode for SecretString {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(SecretString::new(String::decode(input)?))
    }
}

// ---- wire error code ---------------------------------------------------------------------------

impl WireEncode for WireErrorCode {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u16(*self as u16);
        Ok(())
    }
}

impl WireDecode for WireErrorCode {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let code = get_u16(input)?;
        WireErrorCode::from_u16(code).ok_or_else(|| {
            StorageError::protocol(format!("unknown error code {code}"))
        })
    }
}

// ---- Vec<u8> for free-form byte payloads -------------------------------------------------------

impl WireEncode for Vec<u8> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self)
    }
}

impl WireDecode for Vec<u8> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        // Byte fields use the full frame budget as their cap; they are bounded by the outer frame
        // size enforced in [`crate::transport`].
        super::traits::get_bytes(input, super::super::limits::MAX_FRAME_BYTES)
    }
}

// ---- Vec<String> for bounded bulk-delete requests ---------------------------------------------

impl WireEncode for Vec<String> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let limit = super::super::limits::MAX_BULK_DELETE_OBJECT_KEYS;
        if self.len() > limit {
            return Err(StorageError::resource_exhausted(format!(
                "bulk-delete request exceeds {limit} keys"
            )));
        }
        out.put_u32(self.len() as u32);
        for value in self {
            value.encode(out)?;
        }
        Ok(())
    }
}

impl WireDecode for Vec<String> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let len = get_u32(input)? as usize;
        let limit = super::super::limits::MAX_BULK_DELETE_OBJECT_KEYS;
        if len > limit {
            return Err(StorageError::resource_exhausted(format!(
                "bulk-delete request exceeds {limit} keys"
            )));
        }
        // Every encoded string contains at least its four-byte length prefix.
        if len > input.remaining() / std::mem::size_of::<u32>() {
            return Err(StorageError::protocol(format!(
                "bulk-delete request declares {len} keys but only {} bytes remain",
                input.remaining()
            )));
        }
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(String::decode(input)?);
        }
        Ok(values)
    }
}

// ---- u32 / u64 scalar types --------------------------------------------------------------------

impl WireEncode for u32 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u32(*self);
        Ok(())
    }
}

impl WireDecode for u32 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        get_u32(input)
    }
}

impl WireEncode for u64 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_u64(*self);
        Ok(())
    }
}

impl WireDecode for u64 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        get_u64(input)
    }
}

impl WireEncode for i64 {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        out.put_i64(*self);
        Ok(())
    }
}

impl WireDecode for i64 {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        if input.remaining() < std::mem::size_of::<i64>() {
            return Err(StorageError::protocol("truncated i64 field"));
        }
        Ok(input.get_i64())
    }
}

// ---- list cursor / entry -----------------------------------------------------------------------
//
// `ListCursor` is a length-prefixed string newtype (server-issued opaque token).
// `WireListEntry` is encoded as `(key, size, etag, last_modified_ms)`.
// bucket/key carrying message in this codec.

impl WireEncode for ListCursor {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        super::traits::put_bytes(out, self.as_str().as_bytes())
    }
}

impl WireDecode for ListCursor {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self::from_wire(String::decode(input)?))
    }
}

impl WireEncode for WireListEntry {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.key.encode(out)?;
        self.size.encode(out)?;
        self.etag.encode(out)?;
        self.last_modified_ms.encode(out)
    }
}

impl WireDecode for WireListEntry {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            key: WireDecode::decode(input)?,
            size: WireDecode::decode(input)?,
            etag: WireDecode::decode(input)?,
            last_modified_ms: WireDecode::decode(input)?,
        })
    }
}

// ---- Vec<WireListEntry> ------------------------------------------------------------------------
//
// `Vec<u8>` already has a wire impl above; for `Vec<WireListEntry>` (the only other vec-of-T on
// the wire today) we use a length prefix + concatenated element encodings. The cap is the same
// outer-frame budget so a single page cannot exceed the framing limit.

impl WireEncode for Vec<WireListEntry> {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        let len = u32::try_from(self.len()).map_err(|_| {
            StorageError::protocol("list response too large to encode")
        })?;
        out.put_u32(len);
        for entry in self {
            entry.encode(out)?;
        }
        Ok(())
    }
}

impl WireDecode for Vec<WireListEntry> {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        let len = get_u32(input)? as usize;
        // Guard against a corrupted frame asking us to allocate a huge vec; one entry is at
        // least 1 byte (length-prefixed empty key + size + etag tag), so an entry count above the
        // remaining frame size is necessarily a malformed frame.
        if len > input.remaining() {
            return Err(StorageError::protocol(format!(
                "list response declares {len} entries but only {} bytes remain",
                input.remaining()
            )));
        }
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(WireListEntry::decode(input)?);
        }
        Ok(entries)
    }
}
