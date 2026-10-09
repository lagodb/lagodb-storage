//! Credential configuration on the wire.
use super::traits::{WireDecode, WireEncode, get_u8};
use crate::backend::{
    AwsAssumeRole, AwsCredentialConfig, AwsCredentialSource, S3Authentication,
};
use crate::error::{StorageError, StorageResult};
use bytes::{Buf, BufMut};

impl WireEncode for AwsAssumeRole {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.role_arn.encode(out)?;
        self.external_id.encode(out)?;
        self.session_name.encode(out)
    }
}

impl WireDecode for AwsAssumeRole {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            role_arn: WireDecode::decode(input)?,
            external_id: WireDecode::decode(input)?,
            session_name: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for AwsCredentialConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.source.encode(out)?;
        self.assume_role.encode(out)
    }
}

impl WireDecode for AwsCredentialConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            source: WireDecode::decode(input)?,
            assume_role: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for AwsCredentialSource {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::DefaultChain => {
                out.put_u8(0);
                Ok(())
            }
            Self::Profile { name } => {
                out.put_u8(1);
                name.encode(out)
            }
            Self::AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } => {
                out.put_u8(2);
                access_key_id.encode(out)?;
                secret_access_key.encode(out)?;
                token.encode(out)
            }
        }
    }
}
impl WireDecode for AwsCredentialSource {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            0 => Ok(Self::DefaultChain),
            1 => Ok(Self::Profile {
                name: WireDecode::decode(input)?,
            }),
            2 => Ok(Self::AccessKey {
                access_key_id: WireDecode::decode(input)?,
                secret_access_key: WireDecode::decode(input)?,
                token: WireDecode::decode(input)?,
            }),
            tag => Err(StorageError::protocol(format!(
                "unknown AWS credential source {tag}"
            ))),
        }
    }
}
impl WireEncode for S3Authentication {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::Anonymous => {
                out.put_u8(0);
                Ok(())
            }
            Self::Aws(config) => {
                out.put_u8(1);
                config.encode(out)
            }
        }
    }
}
impl WireDecode for S3Authentication {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            0 => Ok(Self::Anonymous),
            1 => Ok(Self::Aws(WireDecode::decode(input)?)),
            tag => Err(StorageError::protocol(format!(
                "unknown S3 authentication {tag}"
            ))),
        }
    }
}
