//! AWS SDK credential discovery adapted to object_store's signing boundary.

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AwsCredential, AwsCredentialProvider};
use object_store::{CredentialProvider, Error, Result};
use tokio::sync::Mutex;

/// One AWS provider shared by all bucket and upload clients of a
/// configured backend. Construction runs on the storage worker's runtime.
pub(super) struct AwsCredentials {
    region: String,
    state: RwLock<CredentialState>,
    refresh: Mutex<()>,
}

mod source;
use super::credentials::AwsCredentialConfig;
use source::AwsSource;

struct CredentialState {
    source: SharedCredentialsProvider,
    cached: Option<CachedCredentials>,
}

#[derive(Clone)]
struct CachedCredentials {
    value: Arc<AwsCredential>,
    refresh_at: Option<Instant>,
    expires_at: Option<SystemTime>,
}

impl CachedCredentials {
    fn new(credentials: Credentials) -> Self {
        let refresh_at = credentials.expiry().map(|expires_at| {
            let now = Instant::now();
            let ttl = expires_at
                .duration_since(SystemTime::now())
                .unwrap_or_default();
            // Bound the lead time by the lifetime: short-lived credentials
            // must not immediately enter another refresh cycle.
            let lead = (ttl / 10).min(Duration::from_secs(60));
            now + (ttl - lead)
        });
        Self {
            value: Arc::new(AwsCredential {
                key_id: credentials.access_key_id().to_owned(),
                secret_key: credentials.secret_access_key().to_owned(),
                token: credentials.session_token().map(str::to_owned),
            }),
            refresh_at,
            expires_at: credentials.expiry(),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|expiry| SystemTime::now() >= expiry)
    }

    fn needs_refresh(&self) -> bool {
        self.refresh_at
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
}

impl AwsCredentials {
    pub(super) async fn load(
        config: &AwsCredentialConfig,
        region: Option<&str>,
    ) -> Self {
        let source = AwsSource::load(config, region).await;
        Self {
            region: source.region,
            state: RwLock::new(CredentialState {
                source: source.provider,
                cached: None,
            }),
            refresh: Mutex::new(()),
        }
    }

    pub(super) fn region(&self) -> &str {
        &self.region
    }

    /// Signing updates credentials only; the backend owns the resolved address.
    pub(super) fn apply(
        self: &Arc<Self>,
        builder: AmazonS3Builder,
    ) -> AmazonS3Builder {
        builder.with_credentials(Arc::clone(self) as AwsCredentialProvider)
    }

    fn cached_credentials(&self) -> Option<CachedCredentials> {
        self.state
            .read()
            .expect("AWS credential cache rwlock poisoned")
            .cached
            .clone()
    }

    /// Missing or expired credentials require a completed load. Recheck after
    /// acquiring the lock so waiters reuse the first successful load.
    async fn must_refresh(&self) -> Result<Arc<AwsCredential>> {
        let _refresh = self.refresh.lock().await;
        if let Some(cached) = self.cached_credentials()
            && !cached.is_expired()
        {
            return Ok(cached.value);
        }
        self.load_credentials().await
    }

    /// Early refresh is optional while the cached identity is still valid.
    /// Other requests keep their snapshot instead of waiting for this load.
    ///
    /// Deliberate tradeoff: no cache-level retry cooldown. Within the early
    /// refresh window (10% of the loaded lifetime, capped at 60 seconds), a
    /// failed attempt leaves the next request eligible to try again. Only one
    /// provider load runs at a time; its caller waits while concurrent callers
    /// reuse unexpired credentials. We accept repeated sequential attempts to
    /// pick up provider recovery promptly without another backoff policy or
    /// scheduling state. This window does not bound an individual load's
    /// duration; once credentials expire, callers use mandatory refresh.
    async fn maybe_refresh(
        &self,
        cached: CachedCredentials,
    ) -> Result<Arc<AwsCredential>> {
        let Ok(_refresh) = self.refresh.try_lock() else {
            return Ok(cached.value);
        };
        if let Some(current) = self.cached_credentials()
            && !Arc::ptr_eq(&current.value, &cached.value)
            && !current.is_expired()
        {
            return Ok(current.value);
        }
        match self.load_credentials().await {
            Ok(credentials) => Ok(credentials),
            // The old snapshot can expire while the provider call is in flight.
            Err(_) if !cached.is_expired() => Ok(cached.value),
            Err(error) => Err(error),
        }
    }

    /// The caller holds the refresh lock. Network I/O never holds the cache lock.
    ///
    /// Policy: credential-loading retries and timeout behavior belong to the
    /// AWS SDK/provider. This adapter deliberately adds no independent deadline
    /// that could cut short the provider's own loading and retry lifecycle.
    /// Provider completion is an accepted dependency, not a guarantee of the
    /// raw ProvideCredentials interface: some sources, such as credential_process,
    /// have no load deadline, and this direct call does not inherit the SDK's
    /// identity-cache timeout. A stalled load retains the refresh lock and
    /// blocks mandatory refresh callers. Address such failures at the provider
    /// configuration or SDK integration boundary, keeping this policy explicit.
    async fn load_credentials(&self) -> Result<Arc<AwsCredential>> {
        let source = self
            .state
            .read()
            .expect("AWS credential state rwlock poisoned")
            .source
            .clone();
        let credentials =
            source
                .provide_credentials()
                .await
                .map_err(|source| Error::Generic {
                    store: "S3",
                    source: Box::new(source),
                })?;
        let cached = CachedCredentials::new(credentials);
        let credentials = Arc::clone(&cached.value);
        self.state
            .write()
            .expect("AWS credential state rwlock poisoned")
            .cached = Some(cached);
        Ok(credentials)
    }
}

impl fmt::Debug for AwsCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsCredentials")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for AwsCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        match self.cached_credentials() {
            None => self.must_refresh().await,
            Some(cached) if cached.is_expired() => self.must_refresh().await,
            Some(cached) if cached.needs_refresh() => {
                self.maybe_refresh(cached).await
            }
            Some(cached) => Ok(cached.value),
        }
    }
}

#[cfg(test)]
mod tests;
