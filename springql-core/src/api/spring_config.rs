// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::Deserialize;

use crate::api::error::{Result, SpringError};

/// Default configuration.
///
/// Default key-values are overwritten by `overwrite_config_toml` parameter in `SpringConfig::new()`.
const SPRING_CONFIG_DEFAULT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/default_config.toml"));

/// Top-level config.
#[allow(missing_docs)]
#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringConfig {
    pub worker: SpringWorkerConfig,
    pub memory: SpringMemoryConfig,
    pub web_console: SpringWebConsoleConfig,
    pub source_reader: SpringSourceReaderConfig,
    pub sink_writer: SpringSinkWriterConfig,
}

impl Default for SpringConfig {
    fn default() -> Self {
        Self::new("").expect("default configuration must be valid")
    }
}

impl SpringConfig {
    /// # Failures
    ///
    /// - [SpringError::InvalidConfig](crate::api::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::api::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub fn new(overwrite_config_toml: &str) -> Result<Self> {
        let default_conf = config::Config::builder()
            .add_source(config::File::from_str(
                SPRING_CONFIG_DEFAULT,
                config::FileFormat::Toml,
            ))
            .build()
            .expect("SPRING_CONFIG_DEFAULT is in wrong format");

        let c = config::Config::builder()
            .add_source(default_conf)
            .add_source(config::File::from_str(
                overwrite_config_toml,
                config::FileFormat::Toml,
            ))
            .build()
            .map_err(|e| SpringError::InvalidFormat {
                s: overwrite_config_toml.to_string(),
                source: e.into(),
            })?;

        c.try_deserialize()
            .map_err(|e| SpringError::InvalidConfig { source: e.into() })
    }

    /// Configuration by TOML format string.
    ///
    /// # Parameters
    ///
    /// - `overwrite_config_toml`: TOML format configuration to overwrite default. See `SPRING_CONFIG_DEFAULT` in [spring_config.rs](https://github.com/SpringQL/SpringQL/tree/main/springql-core/src/api/spring_config.rs) for full-set default configuration.
    ///
    /// # Failures
    ///
    /// - [SpringError::InvalidConfig](crate::api::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::api::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub fn from_toml(overwrite_config_toml: &str) -> Result<SpringConfig> {
        SpringConfig::new(overwrite_config_toml)
    }
}

/// Config related to worker threads.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringWorkerConfig {
    pub n_generic_worker_threads: u16,
    pub n_source_worker_threads: u16,
    pub sleep_msec_no_row: u64,
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

    pub memory_state_transition_interval_msec: u32,
    pub performance_metrics_summary_report_interval_msec: u32,
}

/// Config related to web console.
#[allow(missing_docs)]
#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringWebConsoleConfig {
    pub enable_report_post: bool,

    pub report_interval_msec: u32,

    pub host: String,
    pub port: u16,

    pub timeout_msec: u32,
}

/// Config related to source reader
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringSourceReaderConfig {
    pub net_connect_timeout_msec: u32,
    pub net_read_timeout_msec: u32,

    pub can_read_timeout_msec: u32,
}

/// Config related to sink writer.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringSinkWriterConfig {
    pub net_connect_timeout_msec: u32,
    pub net_write_timeout_msec: u32,

    pub http_timeout_msec: u32,
    pub http_connect_timeout_msec: u32,
}
