use std::sync::Arc;

use super::{ManagedStoreRegistry, ObjectBackend, StoreConfig};
use crate::backend::{AwsCredentialOptions, S3Authentication, S3StoreConfig};
use crate::error::StorageErrorKind;
use crate::object::BackendDataIdentity;

#[tokio::test]
async fn refresh_preserves_sharing_and_creates_a_new_backend_each_batch() {
    let registry = ManagedStoreRegistry::new();
    let config = StoreConfig::S3(S3StoreConfig {
        region: Some("us-east-1".to_owned()),
        authentication: S3Authentication::Anonymous,
        ..S3StoreConfig::default()
    });
    registry.upsert_config(1, config.clone()).await.unwrap();
    registry.upsert_config(2, config.clone()).await.unwrap();
    let first_slot = registry.resolve(1).unwrap();
    let second_slot = registry.resolve(2).unwrap();
    assert!(Arc::ptr_eq(&first_slot.backend(), &second_slot.backend()));

    for _ in 0..2 {
        let before = first_slot.backend();
        let old_backend = Arc::downgrade(&before);
        {
            let mut refresh = registry.begin_refresh();
            refresh.refresh_config(1, config.clone()).await.unwrap();
            refresh.refresh_config(2, config.clone()).await.unwrap();
        }

        let after = first_slot.backend();
        assert!(!Arc::ptr_eq(&before, &after));
        assert!(Arc::ptr_eq(&after, &second_slot.backend()));
        // An operation holding the old Arc can finish after both slots change.
        assert!(old_backend.upgrade().is_some());
        drop(before);
        assert!(old_backend.upgrade().is_none());
    }

    registry.upsert_config(3, config).await.unwrap();
    assert!(Arc::ptr_eq(
        &first_slot.backend(),
        &registry.resolve(3).unwrap().backend(),
    ));
}

#[tokio::test]
async fn fresh_and_reused_backends_publish_the_resolved_identity() {
    let registry = ManagedStoreRegistry::new();
    // Anonymous S3 resolves a missing region to us-east-1 without consulting
    // environment credentials or accessing the network.
    let mut s3 = S3StoreConfig {
        authentication: S3Authentication::Anonymous,
        ..S3StoreConfig::default()
    };
    let requested = StoreConfig::S3(s3.clone());
    registry.upsert_config(1, requested.clone()).await.unwrap();
    registry.upsert_config(2, requested.clone()).await.unwrap();

    s3.region = Some("us-east-1".to_owned());
    let resolved_identity = BackendDataIdentity::from_config(&StoreConfig::S3(s3));
    assert_ne!(
        resolved_identity,
        BackendDataIdentity::from_config(&requested),
    );

    let mut refresh = registry.begin_refresh();
    // Both publications must pass the existing slots' resolved identity check.
    refresh.refresh_config(1, requested.clone()).await.unwrap();
    refresh.refresh_config(2, requested).await.unwrap();
    drop(refresh);

    let first_slot = registry.resolve(1).unwrap();
    let second_slot = registry.resolve(2).unwrap();
    assert_eq!(first_slot.identity(), &resolved_identity);
    assert_eq!(second_slot.identity(), &resolved_identity);
    assert!(Arc::ptr_eq(&first_slot.backend(), &second_slot.backend()));
}

#[tokio::test]
async fn publication_conflict_keeps_the_old_slot_and_reuses_the_prepared_backend() {
    let registry = ManagedStoreRegistry::new();
    let mut s3 = S3StoreConfig {
        region: Some("us-west-2".to_owned()),
        authentication: S3Authentication::Anonymous,
        ..S3StoreConfig::default()
    };
    registry
        .upsert_config(1, StoreConfig::S3(s3.clone()))
        .await
        .unwrap();
    let old_slot = registry.resolve(1).unwrap();
    let old_backend = old_slot.backend();
    s3.region = Some("us-east-1".to_owned());
    let replacement = StoreConfig::S3(s3);

    let mut refresh = registry.begin_refresh();
    let error = refresh
        .refresh_config(1, replacement.clone())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Conflict);
    assert!(Arc::ptr_eq(&old_backend, &old_slot.backend()));
    assert_eq!(refresh.backends.len(), 1);
    let prepared = Arc::clone(&refresh.backends[0]);
    let prepared: Arc<dyn ObjectBackend> = prepared;

    refresh.refresh_config(2, replacement).await.unwrap();
    assert!(Arc::ptr_eq(
        &prepared,
        &registry.resolve(2).unwrap().backend()
    ));
    drop(refresh);
    assert!(Arc::ptr_eq(
        &prepared,
        &registry.resolve(2).unwrap().backend()
    ));
    assert!(Arc::ptr_eq(&old_backend, &old_slot.backend()));
}

#[tokio::test]
async fn dropping_a_failed_batch_releases_its_unpublished_backend() {
    let registry = ManagedStoreRegistry::new();
    let mut s3 = S3StoreConfig {
        region: Some("us-west-2".to_owned()),
        authentication: S3Authentication::Anonymous,
        ..S3StoreConfig::default()
    };
    registry
        .upsert_config(1, StoreConfig::S3(s3.clone()))
        .await
        .unwrap();
    s3.region = Some("us-east-1".to_owned());

    let unpublished = {
        let mut refresh = registry.begin_refresh();
        let error = refresh
            .refresh_config(1, StoreConfig::S3(s3))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Conflict);
        assert_eq!(refresh.backends.len(), 1);
        Arc::downgrade(&refresh.backends[0])
    };
    assert!(unpublished.upgrade().is_none());
}

#[tokio::test]
async fn equal_physical_identities_with_different_secrets_do_not_share_backends() {
    let registry = ManagedStoreRegistry::new();
    let mut refresh = registry.begin_refresh();
    for (volume_id, secret) in [(1, "first-secret"), (2, "second-secret")] {
        let authentication = AwsCredentialOptions {
            access_key_id: Some("same-access-key"),
            secret_access_key: Some(secret),
            ..AwsCredentialOptions::default()
        }
        .into_authentication(false)
        .unwrap();
        let config = StoreConfig::S3(S3StoreConfig {
            region: Some("us-east-1".to_owned()),
            authentication,
            ..S3StoreConfig::default()
        });
        refresh.refresh_config(volume_id, config).await.unwrap();
    }
    drop(refresh);

    let first_slot = registry.resolve(1).unwrap();
    let second_slot = registry.resolve(2).unwrap();
    assert_eq!(first_slot.identity(), second_slot.identity());
    assert!(!Arc::ptr_eq(&first_slot.backend(), &second_slot.backend()));
}
