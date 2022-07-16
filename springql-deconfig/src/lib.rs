use serde::Deserialize;
pub use springql_core::api::{
    Result, SpringConfig, SpringError, SpringMemoryConfig, SpringSinkWriterConfig,
    SpringSourceReaderConfig, SpringWebConsoleConfig, SpringWorkerConfig,
};

const SPRING_CONFIG_DEFAULT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../default_config.toml"
));

#[allow(missing_docs)]
#[derive(Clone, Eq, PartialEq, Debug, Deserialize)]
struct SpringConfigDeserialize {
    #[serde(with = "SpringWorkerConfigDeserialize")]
    pub worker: SpringWorkerConfig,
    #[serde(with = "SpringMemoryConfigDeserialize")]
    pub memory: SpringMemoryConfig,
    #[serde(with = "SpringWebConsoleConfigDeserialize")]
    pub web_console: SpringWebConsoleConfig,
    #[serde(with = "SpringSourceReaderConfigDeserialize")]
    pub source_reader: SpringSourceReaderConfig,
    #[serde(with = "SpringSinkWriterConfigDeserialize")]
    pub sink_writer: SpringSinkWriterConfig,
}

#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringWorkerConfig")]
struct SpringWorkerConfigDeserialize {
    pub n_generic_worker_threads: u16,
    pub n_source_worker_threads: u16,
    pub sleep_msec_no_row: u64,
}

#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringMemoryConfig")]
struct SpringMemoryConfigDeserialize {
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
#[serde(remote = "SpringWebConsoleConfig")]
struct SpringWebConsoleConfigDeserialize {
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

/// Config related to sink writer.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Deserialize)]
#[serde(remote = "SpringSinkWriterConfig")]
pub struct SpringSinkWriterConfigDeserialize {
    pub net_connect_timeout_msec: u32,
    pub net_write_timeout_msec: u32,

    pub http_timeout_msec: u32,
    pub http_connect_timeout_msec: u32,
}

impl SpringConfigDeserialize {
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
}

/// trait for deserialize configuration from file
pub trait SpringConfigExt {
    /// Create StringConfig from toml
    fn from_toml(toml: &str) -> Result<SpringConfig>;
}

impl SpringConfigExt for SpringConfig {
    fn from_toml(toml: &str) -> Result<SpringConfig> {
        SpringConfigDeserialize::from_toml(toml)
    }
}
