use std::sync::Arc;

use super::config::StoreConfig;
use crate::object::{BackendDataIdentity, BackendDataIdentityKind};

impl BackendDataIdentity {
    pub(crate) fn from_config(config: &StoreConfig) -> Self {
        let kind = match config {
            StoreConfig::S3(config) => BackendDataIdentityKind::S3 {
                region: config.region.as_deref().map(Arc::from),
                endpoint: config.endpoint.as_deref().map(Arc::from),
            },
            StoreConfig::S3Compatible(config) => {
                BackendDataIdentityKind::S3Compatible {
                    endpoint: Arc::from(config.endpoint.as_str()),
                }
            }
            StoreConfig::Gcs(config) => BackendDataIdentityKind::Gcs {
                base_url: config.base_url.as_deref().map(Arc::from),
            },
            StoreConfig::Azure(config) => BackendDataIdentityKind::Azure {
                account: config.account.as_deref().map(Arc::from),
                endpoint: config.endpoint.as_deref().map(Arc::from),
                use_emulator: config.use_emulator,
            },
        };
        Self::from_kind(kind)
    }
}
