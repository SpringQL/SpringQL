use serde::Deserialize;

use crate::error::{Result, SpringError};

/// Default configuration.
///
/// Default key-values are overwritten by `overwrite_config` parameter in `spring_open()`.
const SPRING_CONFIG_DEFAULT: &str = r#"
[memory]
# How much memory is allowed to be used in SpringQL streaming runtime.
upper_limit_bytes = 10_000_000

# Percentage over `upper_limit_bytes` to transit from Moderate state to Severe.
# In Severe state, internal scheduler is changed to exhibit memory-resilience.
moderate_to_severe_percent = 60

# Percentage over `upper_limit_bytes` to transit from Severe state to Critical.
# In Critical state, all intermediate rows are purged to release memory.
severe_to_critical_percent = 95

critical_to_severe_percent = 80
severe_to_moderate_percent = 40
"#;

#[derive(Eq, PartialEq, Debug, Deserialize)]
pub(crate) struct SpringConfig {
    pub(crate) memory: Memory,
}

impl SpringConfig {
    /// # Failures
    ///
    /// - [SpringError::InvalidConfig](crate::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub(crate) fn new(overwrite_config_toml: &str) -> Result<Self> {
        let mut c = config::Config::new();

        c.merge(config::File::from_str(
            SPRING_CONFIG_DEFAULT,
            config::FileFormat::Toml,
        ))
        .expect("SPRING_CONFIG_DEFAULT is in wrong format");

        c.merge(config::File::from_str(
            overwrite_config_toml,
            config::FileFormat::Toml,
        ))
        .map_err(|e| SpringError::InvalidFormat {
            s: overwrite_config_toml.to_string(),
            source: e.into(),
        })?;

        c.try_into()
            .map_err(|e| SpringError::InvalidConfig { source: e.into() })
    }
}

#[derive(Eq, PartialEq, Debug, Deserialize)]
pub(crate) struct Memory {
    pub(crate) upper_limit_bytes: u64,

    pub(crate) moderate_to_severe_bytes: u64,
    pub(crate) severe_to_critical_bytes: u64,

    pub(crate) critical_to_severe_bytes: u64,
    pub(crate) severe_to_moderate_bytes: u64,
}
