//! Credential configuration shared by every S3 configuration adapter.

use serde::{Deserialize, Serialize};

use super::SecretString;
use super::config::{
    validate_non_empty, validate_optional_non_empty, validate_optional_secret,
};
use crate::error::{StorageError, StorageResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3Authentication {
    Anonymous,
    Aws(AwsCredentialConfig),
}

impl Default for S3Authentication {
    fn default() -> Self {
        Self::Aws(AwsCredentialConfig::default())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentialConfig {
    #[serde(default)]
    pub source: AwsCredentialSource,
    #[serde(default)]
    pub assume_role: Option<AwsAssumeRole>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AwsCredentialSource {
    #[default]
    DefaultChain,
    Profile {
        name: String,
    },
    AccessKey {
        access_key_id: SecretString,
        secret_access_key: SecretString,
        #[serde(default)]
        token: Option<SecretString>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsAssumeRole {
    pub role_arn: String,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default)]
    pub session_name: Option<String>,
}

impl AwsCredentialConfig {
    pub fn validate(&self) -> StorageResult<()> {
        match &self.source {
            AwsCredentialSource::DefaultChain => {}
            AwsCredentialSource::Profile { name } => {
                validate_non_empty("AWS profile", name)?
            }
            AwsCredentialSource::AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } => {
                validate_optional_secret("S3 access_key_id", Some(access_key_id))?;
                validate_optional_secret(
                    "S3 secret_access_key",
                    Some(secret_access_key),
                )?;
                validate_optional_secret("S3 token", token.as_ref())?;
            }
        }
        if let Some(role) = &self.assume_role {
            validate_non_empty("AWS role_arn", &role.role_arn)?;
            validate_optional_non_empty(
                "AWS external_id",
                role.external_id.as_deref(),
            )?;
            validate_optional_non_empty(
                "AWS role_session_name",
                role.session_name.as_deref(),
            )?;
        }
        Ok(())
    }
}

impl S3Authentication {
    pub fn aws_config(&self) -> Option<&AwsCredentialConfig> {
        match self {
            Self::Aws(config) => Some(config),
            Self::Anonymous => None,
        }
    }

    pub fn validate(&self) -> StorageResult<()> {
        match self {
            Self::Aws(config) => config.validate(),
            Self::Anonymous => Ok(()),
        }
    }
}

/// Borrowed SQL/property fields. Source selection and conflicts are checked
/// once at the configuration boundary, independently of the entry point.
#[derive(Default)]
pub struct AwsCredentialOptions<'a> {
    pub access_key_id: Option<&'a str>,
    pub secret_access_key: Option<&'a str>,
    pub token: Option<&'a str>,
    pub aws_profile: Option<&'a str>,
    pub role_arn: Option<&'a str>,
    pub external_id: Option<&'a str>,
    pub role_session_name: Option<&'a str>,
}
impl AwsCredentialOptions<'_> {
    pub fn is_configured(&self) -> bool {
        self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.token.is_some()
            || self.aws_profile.is_some()
            || self.role_arn.is_some()
            || self.external_id.is_some()
            || self.role_session_name.is_some()
    }

    pub fn validate(&self) -> StorageResult<()> {
        if self.access_key_id.is_some() != self.secret_access_key.is_some()
            || (self.token.is_some() && self.access_key_id.is_none())
        {
            return Err(StorageError::configuration(
                "S3 credentials require access_key_id and secret_access_key together; token requires both",
            ));
        }
        if self.aws_profile.is_some() && self.access_key_id.is_some() {
            return Err(StorageError::configuration(
                "AWS profile and explicit S3 credentials are mutually exclusive",
            ));
        }
        if self.role_arn.is_none()
            && (self.external_id.is_some() || self.role_session_name.is_some())
        {
            return Err(StorageError::configuration(
                "external_id and role_session_name require role_arn",
            ));
        }
        for (name, value) in [
            ("S3 access_key_id", self.access_key_id),
            ("S3 secret_access_key", self.secret_access_key),
            ("S3 token", self.token),
            ("AWS profile", self.aws_profile),
            ("AWS role_arn", self.role_arn),
            ("AWS external_id", self.external_id),
            ("AWS role_session_name", self.role_session_name),
        ] {
            validate_optional_non_empty(name, value)?;
        }
        Ok(())
    }

    pub fn into_authentication(
        self,
        anonymous: bool,
    ) -> StorageResult<S3Authentication> {
        self.validate()?;
        if anonymous {
            if self.is_configured() {
                return Err(StorageError::configuration(
                    "anonymous S3 access cannot be combined with a credential source or AssumeRole",
                ));
            }
            return Ok(S3Authentication::Anonymous);
        }
        let source =
            match (self.access_key_id, self.secret_access_key, self.aws_profile) {
                (None, None, None) => AwsCredentialSource::DefaultChain,
                (None, None, Some(name)) => AwsCredentialSource::Profile {
                    name: name.to_owned(),
                },
                (Some(key), Some(secret), None) => AwsCredentialSource::AccessKey {
                    access_key_id: SecretString::new(key),
                    secret_access_key: SecretString::new(secret),
                    token: self.token.map(SecretString::new),
                },
                _ => unreachable!("validated AWS source fields"),
            };
        let assume_role = self.role_arn.map(|role_arn| AwsAssumeRole {
            role_arn: role_arn.to_owned(),
            external_id: self.external_id.map(str::to_owned),
            session_name: self.role_session_name.map(str::to_owned),
        });
        Ok(S3Authentication::Aws(AwsCredentialConfig {
            source,
            assume_role,
        }))
    }
}

#[cfg(test)]
mod tests;
