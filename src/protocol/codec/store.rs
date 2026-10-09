//! Store configuration wire format.
use super::traits::{WireDecode, WireEncode, get_u8};
use crate::backend::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3Encryption,
    S3StoreConfig, StoreConfig,
};
use crate::error::{StorageError, StorageResult};
use bytes::{Buf, BufMut};

// ---- store configs -----------------------------------------------------------------------------
//
// A 1-byte tag selects the variant so new configs can be added without touching callers that only
// handle existing variants.

impl WireEncode for S3Encryption {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::S3 => {
                out.put_u8(1);
                Ok(())
            }
            Self::Kms { key_id } => {
                out.put_u8(2);
                key_id.encode(out)
            }
            Self::Custom { key } => {
                out.put_u8(3);
                key.encode(out)
            }
        }
    }
}

impl WireDecode for S3Encryption {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            1 => Ok(Self::S3),
            2 => Ok(Self::Kms {
                key_id: WireDecode::decode(input)?,
            }),
            3 => Ok(Self::Custom {
                key: WireDecode::decode(input)?,
            }),
            other => Err(StorageError::protocol(format!(
                "unknown S3 encryption type {other}"
            ))),
        }
    }
}

impl WireEncode for StoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        match self {
            Self::S3(config) => {
                out.put_u8(1);
                config.encode(out)
            }
            Self::S3Compatible(config) => {
                out.put_u8(2);
                config.encode(out)
            }
            Self::Gcs(config) => {
                out.put_u8(3);
                config.encode(out)
            }
            Self::Azure(config) => {
                out.put_u8(4);
                config.encode(out)
            }
        }
    }
}

impl WireDecode for StoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        match get_u8(input)? {
            1 => Ok(Self::S3(S3StoreConfig::decode(input)?)),
            2 => Ok(Self::S3Compatible(S3CompatibleStoreConfig::decode(input)?)),
            3 => Ok(Self::Gcs(GcsStoreConfig::decode(input)?)),
            4 => Ok(Self::Azure(AzureStoreConfig::decode(input)?)),
            other => Err(StorageError::protocol(format!(
                "unknown store config type {other}"
            ))),
        }
    }
}

impl WireEncode for S3StoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.region.encode(out)?;
        self.endpoint.encode(out)?;
        self.authentication.encode(out)?;
        self.allow_http.encode(out)?;
        self.virtual_hosted_style_request.encode(out)?;
        self.encryption.encode(out)
    }
}

impl WireDecode for S3StoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            region: WireDecode::decode(input)?,
            endpoint: WireDecode::decode(input)?,
            authentication: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            virtual_hosted_style_request: WireDecode::decode(input)?,
            encryption: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for S3CompatibleStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.endpoint.encode(out)?;
        self.region.encode(out)?;
        self.authentication.encode(out)?;
        self.allow_http.encode(out)?;
        self.virtual_hosted_style_request.encode(out)?;
        self.encryption.encode(out)
    }
}

impl WireDecode for S3CompatibleStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            endpoint: WireDecode::decode(input)?,
            region: WireDecode::decode(input)?,
            authentication: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            virtual_hosted_style_request: WireDecode::decode(input)?,
            encryption: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for GcsStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.base_url.encode(out)?;
        self.service_account_path.encode(out)?;
        self.service_account_key.encode(out)?;
        self.application_credentials_path.encode(out)?;
        self.bearer_token.encode(out)?;
        self.skip_signature.encode(out)
    }
}

impl WireDecode for GcsStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            base_url: WireDecode::decode(input)?,
            service_account_path: WireDecode::decode(input)?,
            service_account_key: WireDecode::decode(input)?,
            application_credentials_path: WireDecode::decode(input)?,
            bearer_token: WireDecode::decode(input)?,
            skip_signature: WireDecode::decode(input)?,
        })
    }
}

impl WireEncode for AzureStoreConfig {
    fn encode(&self, out: &mut impl BufMut) -> StorageResult<()> {
        self.account.encode(out)?;
        self.endpoint.encode(out)?;
        self.access_key.encode(out)?;
        self.bearer_token.encode(out)?;
        self.sas_token.encode(out)?;
        self.client_id.encode(out)?;
        self.client_secret.encode(out)?;
        self.tenant_id.encode(out)?;
        self.authority_host.encode(out)?;
        self.allow_http.encode(out)?;
        self.use_emulator.encode(out)
    }
}

impl WireDecode for AzureStoreConfig {
    fn decode(input: &mut impl Buf) -> StorageResult<Self> {
        Ok(Self {
            account: WireDecode::decode(input)?,
            endpoint: WireDecode::decode(input)?,
            access_key: WireDecode::decode(input)?,
            bearer_token: WireDecode::decode(input)?,
            sas_token: WireDecode::decode(input)?,
            client_id: WireDecode::decode(input)?,
            client_secret: WireDecode::decode(input)?,
            tenant_id: WireDecode::decode(input)?,
            authority_host: WireDecode::decode(input)?,
            allow_http: WireDecode::decode(input)?,
            use_emulator: WireDecode::decode(input)?,
        })
    }
}
