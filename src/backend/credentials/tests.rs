use super::*;
use crate::backend::{BackendPool, S3StoreConfig, StoreConfig};
use crate::object::BackendDataIdentity;
use std::sync::Arc;

#[test]
fn identity_options_cannot_fall_back_after_invalid_configuration() {
    for options in [
        AwsCredentialOptions {
            token: Some("token"),
            ..Default::default()
        },
        AwsCredentialOptions {
            access_key_id: Some("key"),
            ..Default::default()
        },
        AwsCredentialOptions {
            aws_profile: Some("analytics"),
            access_key_id: Some("key"),
            secret_access_key: Some("secret"),
            ..Default::default()
        },
        AwsCredentialOptions {
            external_id: Some("external"),
            ..Default::default()
        },
        AwsCredentialOptions {
            role_session_name: Some("session"),
            ..Default::default()
        },
        AwsCredentialOptions {
            aws_profile: Some(" "),
            ..Default::default()
        },
    ] {
        assert!(options.into_authentication(false).is_err());
    }
    assert!(
        AwsCredentialOptions {
            role_arn: Some("role"),
            ..Default::default()
        }
        .into_authentication(true)
        .is_err()
    );
}

#[tokio::test]
async fn profile_and_role_change_provider_sharing_without_changing_data_identity() {
    let pool = BackendPool::default();
    let first = Arc::new(StoreConfig::S3(S3StoreConfig {
        region: Some("us-east-1".to_owned()),
        authentication: AwsCredentialOptions {
            aws_profile: Some("analytics"),
            ..Default::default()
        }
        .into_authentication(false)
        .unwrap(),
        ..Default::default()
    }));
    let second = Arc::new(StoreConfig::S3(S3StoreConfig {
        region: Some("us-east-1".to_owned()),
        authentication: AwsCredentialOptions {
            aws_profile: Some("analytics"),
            role_arn: Some("arn:aws:iam::123456789012:role/reader"),
            ..Default::default()
        }
        .into_authentication(false)
        .unwrap(),
        ..Default::default()
    }));
    let one = pool.intern(Arc::clone(&first)).await.unwrap();
    let same = pool.intern(Arc::clone(&first)).await.unwrap();
    let other = pool.intern(Arc::clone(&second)).await.unwrap();
    assert!(Arc::ptr_eq(&one, &same));
    assert!(!Arc::ptr_eq(&one, &other));
    assert_eq!(
        BackendDataIdentity::from_config(&first),
        BackendDataIdentity::from_config(&second)
    );
}

#[test]
fn native_profile_allows_region_discovery_before_cache_attachment() {
    let authentication = AwsCredentialOptions {
        aws_profile: Some("analytics"),
        ..Default::default()
    }
    .into_authentication(false)
    .unwrap();
    let config = S3StoreConfig {
        authentication,
        ..Default::default()
    };
    assert!(StoreConfig::S3(config.clone()).validate().is_ok());
    assert!(
        StoreConfig::S3(S3StoreConfig {
            region: Some("us-east-1".to_owned()),
            ..config
        })
        .validate()
        .is_ok()
    );
}
