//! Managed attach control-path tests.

use std::sync::Arc;

use crate::backend::{
    ManagedStoreRegistry, MemoryObjectBackend, S3Authentication, S3StoreConfig,
    StoreConfig,
};
use crate::object::BackendDataIdentity;
use crate::protocol::WireRequestPayload;
use crate::service::StorageService;

use super::fixtures::{TEST_VOLUME_ID, memory_cache};

#[tokio::test]
async fn managed_attach_resolves_the_runtime_owned_slot() {
    let registry = ManagedStoreRegistry::new();
    registry
        .register_backend(
            TEST_VOLUME_ID,
            BackendDataIdentity::memory("managed"),
            Arc::new(MemoryObjectBackend::new()),
        )
        .unwrap();
    let service = StorageService::with_registry(registry, memory_cache());

    let attached = service
        .resolve_attach(WireRequestPayload::AttachManaged {
            volume_id: TEST_VOLUME_ID,
        })
        .await
        .unwrap();

    assert_eq!(attached.identity(), &BackendDataIdentity::memory("managed"));
}

#[tokio::test]
async fn unknown_managed_volume_is_rejected_at_attach() {
    let service =
        StorageService::with_registry(ManagedStoreRegistry::new(), memory_cache());

    let error = match service
        .resolve_attach(WireRequestPayload::AttachManaged { volume_id: 99 })
        .await
    {
        Ok(_) => panic!("unknown managed volume unexpectedly attached"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), crate::error::StorageErrorKind::NotFound);
}

#[tokio::test]
async fn configured_and_managed_attach_share_the_resolved_s3_identity() {
    let config = StoreConfig::S3(S3StoreConfig {
        authentication: S3Authentication::Anonymous,
        ..Default::default()
    });
    let expected =
        BackendDataIdentity::from_config(&StoreConfig::S3(S3StoreConfig {
            region: Some("us-east-1".to_owned()),
            authentication: S3Authentication::Anonymous,
            ..Default::default()
        }));
    let registry = ManagedStoreRegistry::new();
    registry
        .upsert_config(TEST_VOLUME_ID, config.clone())
        .await
        .unwrap();
    let service = StorageService::with_registry(registry, memory_cache());
    let configured = service
        .resolve_attach(WireRequestPayload::AttachConfigured {
            config: Arc::new(config),
        })
        .await
        .unwrap();
    let managed = service
        .resolve_attach(WireRequestPayload::AttachManaged {
            volume_id: TEST_VOLUME_ID,
        })
        .await
        .unwrap();
    assert_eq!(configured.identity(), &expected);
    assert_eq!(managed.identity(), &expected);
}
