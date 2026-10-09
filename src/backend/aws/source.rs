//! AWS SDK provider construction, isolated from the signing cache.
use crate::backend::{AwsCredentialConfig, AwsCredentialSource};
use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::{
    BehaviorVersion, defaults, profile::ProfileFileCredentialsProvider,
    provider_config::ProviderConfig, sts::AssumeRoleProvider,
};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_types::region::Region;

pub(super) struct AwsSource {
    pub region: String,
    pub provider: SharedCredentialsProvider,
}

impl AwsSource {
    pub(super) async fn load(
        config: &AwsCredentialConfig,
        region: Option<&str>,
    ) -> Self {
        // Resolve the address once, independently of credential acquisition. The
        // same fixed region is used by profile/STS providers and S3 clients.
        let region = match region {
            Some(region) => Region::new(region.to_owned()),
            None => {
                let mut chain = DefaultRegionChain::builder();
                if let AwsCredentialSource::Profile { name } = &config.source {
                    chain = chain.profile_name(name);
                }
                chain
                    .build()
                    .region()
                    .await
                    .unwrap_or_else(|| Region::new("us-east-1"))
            }
        };
        let mut loader = defaults(BehaviorVersion::latest()).region(region.clone());
        match &config.source {
            AwsCredentialSource::DefaultChain => {}
            AwsCredentialSource::Profile { name } => {
                // An explicit profile selects that provider, rather than allowing
                // environment access keys to override the selected identity.
                let provider_config =
                    ProviderConfig::default().with_region(Some(region.clone()));
                let provider = ProfileFileCredentialsProvider::builder()
                    .configure(&provider_config)
                    .profile_name(name)
                    .build();
                loader = loader.profile_name(name).credentials_provider(provider);
            }
            AwsCredentialSource::AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } => {
                let credentials = Credentials::new(
                    access_key_id.expose_secret(),
                    secret_access_key.expose_secret(),
                    token.as_ref().map(|token| token.expose_secret().to_owned()),
                    None,
                    "LagoDBStatic",
                );
                if config.assume_role.is_none() {
                    return Self {
                        region: region.as_ref().to_owned(),
                        provider: SharedCredentialsProvider::new(credentials),
                    };
                }
                loader = loader.credentials_provider(credentials);
            }
        }
        let sdk = loader.load().await;
        let mut provider = sdk
            .credentials_provider()
            .expect("AWS configuration installs the selected credentials provider");
        if let Some(role) = &config.assume_role {
            let mut builder =
                AssumeRoleProvider::builder(&role.role_arn).configure(&sdk);
            if let Some(external_id) = &role.external_id {
                builder = builder.external_id(external_id);
            }
            if let Some(session_name) = &role.session_name {
                builder = builder.session_name(session_name);
            }
            provider = SharedCredentialsProvider::new(
                builder.build_from_provider(provider).await,
            );
        }
        Self {
            region: region.as_ref().to_owned(),
            provider,
        }
    }
}
