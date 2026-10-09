use std::sync::atomic::{AtomicUsize, Ordering};

use aws_credential_types::provider::{error::CredentialsError, future};

use super::*;
use crate::backend::{AwsCredentialSource, SecretString};

#[derive(Debug)]
struct MockSource {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

impl MockSource {
    fn provider(
        fail: bool,
        cached: Option<CachedCredentials>,
    ) -> (AwsCredentials, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = AwsCredentials {
            region: "us-east-1".to_owned(),
            state: RwLock::new(CredentialState {
                source: SharedCredentialsProvider::new(Self {
                    calls: Arc::clone(&calls),
                    fail,
                }),
                cached,
            }),
            refresh: Mutex::new(()),
        };
        (provider, calls)
    }

    fn cached(expiry: Option<SystemTime>) -> CachedCredentials {
        CachedCredentials::new(Credentials::new(
            "old-key",
            "old-secret",
            Some("old-token".to_owned()),
            expiry,
            "test",
        ))
    }
}

impl ProvideCredentials for MockSource {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            if self.fail {
                return Err(CredentialsError::provider_error(
                    "test credential failure",
                ));
            }
            Ok(Credentials::new(
                "access-key",
                "secret-key",
                Some("session-token".to_owned()),
                Some(SystemTime::now() + Duration::from_secs(120)),
                "test",
            ))
        })
    }
}

#[tokio::test]
async fn concurrent_requests_share_initial_load_and_expired_refresh() {
    let (provider, calls) = MockSource::provider(false, None);
    let (first, second) =
        tokio::join!(provider.get_credential(), provider.get_credential());
    let first = first.unwrap();
    assert!(Arc::ptr_eq(&first, &second.unwrap()));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(Arc::ptr_eq(
        &first,
        &provider.get_credential().await.unwrap()
    ));

    {
        let mut state = provider.state.write().unwrap();
        let cached = state.cached.as_mut().unwrap();
        cached.refresh_at = Some(Instant::now());
        cached.expires_at = Some(SystemTime::now() - Duration::from_secs(1));
    }
    let (refreshed, concurrent) =
        tokio::join!(provider.get_credential(), provider.get_credential());
    let refreshed = refreshed.unwrap();
    assert!(Arc::ptr_eq(&refreshed, &concurrent.unwrap()));
    assert!(!Arc::ptr_eq(&first, &refreshed));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn early_refresh_keeps_valid_cache_available_to_concurrent_requests() {
    let mut cached =
        MockSource::cached(Some(SystemTime::now() + Duration::from_secs(120)));
    cached.refresh_at = Some(Instant::now());
    let old = Arc::clone(&cached.value);
    let (provider, calls) = MockSource::provider(false, Some(cached));

    let (refreshed, concurrent) =
        tokio::join!(provider.get_credential(), provider.get_credential());
    let refreshed = refreshed.unwrap();
    assert!(!Arc::ptr_eq(&old, &refreshed));
    assert!(Arc::ptr_eq(&old, &concurrent.unwrap()));
    assert!(Arc::ptr_eq(
        &refreshed,
        &provider.get_credential().await.unwrap()
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn early_refresh_failure_returns_unexpired_cache() {
    let mut cached =
        MockSource::cached(Some(SystemTime::now() + Duration::from_secs(120)));
    cached.refresh_at = Some(Instant::now());
    let old = Arc::clone(&cached.value);
    let (provider, calls) = MockSource::provider(true, Some(cached));

    assert!(Arc::ptr_eq(&old, &provider.get_credential().await.unwrap()));
    assert!(Arc::ptr_eq(
        &old,
        &provider.cached_credentials().unwrap().value
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn refresh_failure_does_not_return_missing_or_expired_credentials() {
    for cached in [
        None,
        Some(MockSource::cached(Some(
            SystemTime::now() - Duration::from_secs(1),
        ))),
    ] {
        let (provider, calls) = MockSource::provider(true, cached);
        assert!(provider.get_credential().await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn failed_optional_refresh_rechecks_the_old_snapshot_expiry() {
    let cached = MockSource::cached(Some(SystemTime::now() - Duration::from_secs(1)));
    let (provider, calls) = MockSource::provider(true, Some(cached.clone()));
    assert!(provider.maybe_refresh(cached).await.is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn credentials_without_expiry_do_not_load_again() {
    let cached = MockSource::cached(None);
    let old = Arc::clone(&cached.value);
    let (provider, calls) = MockSource::provider(true, Some(cached));
    assert!(Arc::ptr_eq(&old, &provider.get_credential().await.unwrap()));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn first_static_load_is_lazy_and_cached() {
    let config = AwsCredentialConfig {
        source: AwsCredentialSource::AccessKey {
            access_key_id: SecretString::new("test-key"),
            secret_access_key: SecretString::new("test-secret"),
            token: None,
        },
        assume_role: None,
    };
    let provider = AwsCredentials::load(&config, Some("us-east-1")).await;
    assert!(provider.cached_credentials().is_none());
    let credential = provider.get_credential().await.unwrap();
    let cached = provider.cached_credentials().unwrap();
    assert!(Arc::ptr_eq(&credential, &cached.value));
    assert!(cached.expires_at.is_none());
    assert_eq!(credential.key_id, "test-key");
    assert_eq!(credential.secret_key, "test-secret");
    assert!(Arc::ptr_eq(
        &credential,
        &provider.get_credential().await.unwrap()
    ));
    assert!(!format!("{provider:?}").contains("test-key"));
}
