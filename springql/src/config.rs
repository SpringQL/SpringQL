// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::Deserialize;
use springql_core::api::{
    Result, SpringConfig, SpringError, SpringMemoryConfig, SpringSinkWriterConfig,
    SpringSourceReaderConfig, SpringWebConsoleConfig, SpringWorkerConfig,
};

/// Default configuration.
///
/// Default key-values are overwritten by `overwrite_config_toml` parameter in `SpringConfig::new()`.
const SPRING_CONFIG_DEFAULT: &str = r#"
[worker]
# Number of generic worker threads. Generic worker threads deal with internal and sink tasks.
# Setting this to > 1 may improve throughput but lead to out-of-order stream processing.
n_generic_worker_threads = 1

# Number of source worker threads. Source worker threads collect rows from foreign source.
# Too many number may may cause row fraud in runtime.
# Setting this to > 1 may improve throughput but lead to out-of-order stream processing.
n_source_worker_threads = 1

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

# Interval for MemoryStateMachineWorker to publish TransitPerformanceMetricsSummary event.
memory_state_transition_interval_msec = 10

# Interval for PerformanceMonitorWorker to publish ReportMetricsSummary event.
performance_metrics_summary_report_interval_msec = 10

[web_console]
# Whether to enable POST API request to web console.
enable_report_post = false

report_interval_msec = 3_000

host = "127.0.0.1"
port = 8050

timeout_msec = 3_000

[source_reader]
net_connect_timeout_msec = 1_000
net_read_timeout_msec = 100

can_read_timeout_msec = 100

[sink_writer]
net_connect_timeout_msec = 1_000
net_write_timeout_msec = 100
"#;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
pub struct SpringConfigDeserialize {
    #[serde(with = "SpringWorkerConfigDeserialize")]
    worker: SpringWorkerConfig,
    #[serde(with = "SpringMemoryConfigDeserialize")]
    memory: SpringMemoryConfig,
    #[serde(with = "SpringWebConsoleConfigDeserialize")]
    web_console: SpringWebConsoleConfig,
    #[serde(with = "SpringSourceReaderConfigDeserialize")]
    source_reader: SpringSourceReaderConfig,
    #[serde(with = "SpringSinkWriterConfigDeserialize")]
    sink_writer: SpringSinkWriterConfig,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringWorkerConfig")]
pub struct SpringWorkerConfigDeserialize {
    pub n_generic_worker_threads: u16,
    pub n_source_worker_threads: u16,
}

#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringMemoryConfig")]
pub struct SpringMemoryConfigDeserialize {
    pub upper_limit_bytes: u64,

    pub moderate_to_severe_percent: u8,
    pub severe_to_critical_percent: u8,

    pub critical_to_severe_percent: u8,
    pub severe_to_moderate_percent: u8,

    pub memory_state_transition_interval_msec: u32,
    pub performance_metrics_summary_report_interval_msec: u32,
}

#[allow(missing_docs)]
#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringWebConsoleConfig")]
pub struct SpringWebConsoleConfigDeserialize {
    pub enable_report_post: bool,

    pub report_interval_msec: u32,

    pub host: String,
    pub port: u16,

    pub timeout_msec: u32,
}

#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringSourceReaderConfig")]
pub struct SpringSourceReaderConfigDeserialize {
    pub net_connect_timeout_msec: u32,
    pub net_read_timeout_msec: u32,

    pub can_read_timeout_msec: u32,
}

#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringSinkWriterConfig")]
pub struct SpringSinkWriterConfigDeserialize {
    pub net_connect_timeout_msec: u32,
    pub net_write_timeout_msec: u32,
}

impl SpringConfigDeserialize {
    /// # Failures
    ///
    /// - [SpringError::InvalidConfig](crate::api::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::api::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub fn load(overwrite_config_toml: &str) -> Result<SpringConfig> {
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

        let de = c
            .try_deserialize::<'_, Self>()
            .map_err(|e| SpringError::InvalidConfig { source: e.into() })?;

        Ok(SpringConfig {
            worker: de.worker,
            memory: de.memory,
            web_console: de.web_console,
            source_reader: de.source_reader,
            sink_writer: de.sink_writer,
        })
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
        Self::load(overwrite_config_toml)
    }
}

/// deserialize configration from toml
pub fn config_from_toml(toml: &str) -> Result<SpringConfig> {
    SpringConfigDeserialize::from_toml(toml)
}
