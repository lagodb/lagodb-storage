use super::super::model::WireRequestPayload;
use super::*;
use crate::backend::{AwsCredentialOptions, S3StoreConfig, StoreConfig};
use std::sync::Arc;

#[test]
fn profile_and_role_configured_attach_roundtrip() {
    let auth = AwsCredentialOptions {
        aws_profile: Some("analytics"),
        role_arn: Some("arn:aws:iam::123456789012:role/reader"),
        external_id: Some("lagodb"),
        role_session_name: Some("reader"),
        ..Default::default()
    }
    .into_authentication(false)
    .unwrap();
    let request = WireRequest {
        request_id: 1,
        payload: WireRequestPayload::AttachConfigured {
            config: Arc::new(StoreConfig::S3(S3StoreConfig {
                region: Some("us-east-1".to_owned()),
                authentication: auth,
                ..Default::default()
            })),
        },
    };
    assert_eq!(
        decode_request(&encode_request(&request).unwrap()).unwrap(),
        request
    );
}
