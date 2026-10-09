use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ObjectStore, RetryConfig};

use super::{
    AwsCredentials, S3CompatibleStoreConfig, S3Encryption, S3StoreConfig,
    finish_store_build, validate_endpoint, validate_optional_non_empty,
    validate_optional_secret,
};
use crate::backend::{AwsCredentialSource, S3Authentication};
use crate::error::StorageResult;

impl S3StoreConfig {
    pub(super) fn validate(&self) -> StorageResult<()> {
        validate_optional_non_empty("S3 region", self.region.as_deref())?;
        if let Some(endpoint) = &self.endpoint {
            validate_endpoint("S3 endpoint", endpoint, self.allow_http)?;
        }
        self.authentication.validate()?;
        if let Some(encryption) = &self.encryption {
            encryption.validate()?;
        }
        Ok(())
    }

    pub(super) fn build_store(
        &self,
        bucket: &str,
        retry_config: RetryConfig,
        credentials: Option<&Arc<AwsCredentials>>,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = AmazonS3Builder::new();
        if let Some(credentials) = credentials {
            builder = credentials.apply(builder);
        }
        builder = builder.with_bucket_name(bucket);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        // Synchronous validation builds no SDK runtime. Static credentials are
        // sufficient here; operational clients always inject the shared provider.
        if credentials.is_none()
            && let S3Authentication::Aws(config) = &self.authentication
            && let AwsCredentialSource::AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } = &config.source
        {
            builder = builder
                .with_access_key_id(access_key_id.expose_secret())
                .with_secret_access_key(secret_access_key.expose_secret());
            if let Some(token) = token {
                builder = builder.with_token(token.expose_secret());
            }
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        if self.virtual_hosted_style_request {
            builder = builder.with_virtual_hosted_style_request(true);
        }
        if matches!(self.authentication, S3Authentication::Anonymous) {
            builder = builder.with_skip_signature(true);
        }
        if let Some(encryption) = &self.encryption {
            builder = encryption.apply(builder);
        }
        builder = builder.with_retry(retry_config);
        finish_store_build(builder.build(), "S3", "bucket", bucket)
    }
}

impl S3CompatibleStoreConfig {
    pub(super) fn validate(&self) -> StorageResult<()> {
        validate_endpoint("S3-compatible endpoint", &self.endpoint, self.allow_http)?;
        validate_optional_non_empty("S3-compatible region", self.region.as_deref())?;
        self.authentication.validate()?;
        if let Some(encryption) = &self.encryption {
            encryption.validate()?;
        }
        Ok(())
    }

    pub(super) fn build_store(
        &self,
        bucket: &str,
        retry_config: RetryConfig,
        credentials: Option<&Arc<AwsCredentials>>,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = AmazonS3Builder::new();
        if let Some(credentials) = credentials {
            builder = credentials.apply(builder);
        }
        builder = builder
            .with_bucket_name(bucket)
            .with_endpoint(&self.endpoint);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        // Synchronous validation builds no SDK runtime. Static credentials are
        // sufficient here; operational clients always inject the shared provider.
        if credentials.is_none()
            && let S3Authentication::Aws(config) = &self.authentication
            && let AwsCredentialSource::AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } = &config.source
        {
            builder = builder
                .with_access_key_id(access_key_id.expose_secret())
                .with_secret_access_key(secret_access_key.expose_secret());
            if let Some(token) = token {
                builder = builder.with_token(token.expose_secret());
            }
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        if self.virtual_hosted_style_request {
            builder = builder.with_virtual_hosted_style_request(true);
        }
        if matches!(self.authentication, S3Authentication::Anonymous) {
            builder = builder.with_skip_signature(true);
        }
        if let Some(encryption) = &self.encryption {
            builder = encryption.apply(builder);
        }
        builder = builder.with_retry(retry_config);
        finish_store_build(builder.build(), "S3-compatible", "bucket", bucket)
    }
}

impl S3Encryption {
    fn validate(&self) -> StorageResult<()> {
        match self {
            Self::Kms {
                key_id: Some(key_id),
            } => validate_optional_non_empty("S3 KMS key_id", Some(key_id)),
            Self::Custom { key } => {
                validate_optional_secret("S3 customer encryption key", Some(key))
            }
            Self::S3 | Self::Kms { key_id: None } => Ok(()),
        }
    }

    fn apply(&self, mut builder: AmazonS3Builder) -> AmazonS3Builder {
        match self {
            Self::S3 => {
                let key: AmazonS3ConfigKey = "aws_server_side_encryption"
                    .parse()
                    .expect("object_store documents the static SSE config key");
                builder = builder.with_config(key, "AES256");
            }
            Self::Kms { key_id } => {
                let encryption_key: AmazonS3ConfigKey = "aws_server_side_encryption"
                    .parse()
                    .expect("object_store documents the static SSE config key");
                builder = builder.with_config(encryption_key, "aws:kms");
                if let Some(key_id) = key_id {
                    let kms_key: AmazonS3ConfigKey = "aws_sse_kms_key_id"
                        .parse()
                        .expect("object_store documents the static KMS config key");
                    builder = builder.with_config(kms_key, key_id.as_str());
                }
            }
            Self::Custom { key } => {
                builder = builder.with_ssec_encryption(key.expose_secret());
            }
        }
        builder
    }
}
