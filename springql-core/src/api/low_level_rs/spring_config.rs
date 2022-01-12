use serde::Deserialize;

use crate::error::{Result, SpringError};

/// Default configuration.
///
/// Default key-values are overwritten by `overwrite_config` parameter in `spring_open()`.
const SPRING_CONFIG_DEFAULT: &str = r#"
[worker]
# Number of generic worker threads. Generic worker threads deal with internal and sink tasks.
n_generic_worker_threads = 2

# Number of source worker threads. Source worker threads collect rows from foreign source.
# Too many number may may cause row fraud in runtime.
n_source_worker_threads = 2

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

/// Returns default configuration.
pub fn spring_config_default() -> SpringConfig {
    SpringConfig::new("").expect("default configuration must be valid")
}

/// Configuration by TOML format string.
///
/// # Parameters
///
/// - `overwrite_config_toml`: TOML format configuration to overwrite default. See `SPRING_CONFIG_DEFAULT` in [spring_config.rs](https://github.com/SpringQL/SpringQL/tree/main/springql-core/src/api/low_level_rs/spring_config.rs) for full-set default configuration.
///
/// # Failures
///
/// - [SpringError::InvalidConfig](crate::error::SpringError::InvalidConfig) when:
///   - `overwrite_config_toml` includes invalid key and/or value.
/// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
///   - `overwrite_config_toml` is not valid as TOML.
pub fn spring_config_toml(overwrite_config_toml: &str) -> Result<SpringConfig> {
    SpringConfig::new(overwrite_config_toml)
}

/// Top-level config.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringConfig {
    pub worker: SpringWorkerConfig,
    pub memory: SpringMemoryConfig,
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

/// Config related to worker threads.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringWorkerConfig {
    pub n_generic_worker_threads: u16,
    pub n_source_worker_threads: u16,
}

/// Config related to memory management.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringMemoryConfig {
    pub upper_limit_bytes: u64,

    pub moderate_to_severe_percent: u8,
    pub severe_to_critical_percent: u8,

    pub critical_to_severe_percent: u8,
    pub severe_to_moderate_percent: u8,
}
