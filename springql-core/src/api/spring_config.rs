// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

/// Top-level config.
#[allow(missing_docs)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SpringConfig {
    pub worker: SpringWorkerConfig,
    pub memory: SpringMemoryConfig,
    pub web_console: SpringWebConsoleConfig,
    pub source_reader: SpringSourceReaderConfig,
    pub sink_writer: SpringSinkWriterConfig,
}

impl Default for SpringConfig {
    fn default() -> Self {
        Self {
            worker: SpringWorkerConfig {
                n_generic_worker_threads: 1,
                n_source_worker_threads: 1,
            },
            memory: SpringMemoryConfig {
                upper_limit_bytes: 10_000_000,
                moderate_to_severe_percent: 60,
                severe_to_critical_percent: 95,
                critical_to_severe_percent: 80,
                severe_to_moderate_percent: 40,
                memory_state_transition_interval_msec: 10,
                performance_metrics_summary_report_interval_msec: 10,
            },
            web_console: SpringWebConsoleConfig {
                enable_report_post: false,
                report_interval_msec: 3000,
                host: "127.0.0.1".to_string(),
                port: 8050,
                timeout_msec: 3000,
            },
            source_reader: SpringSourceReaderConfig {
                net_connect_timeout_msec: 1000,
                net_read_timeout_msec: 100,
                can_read_timeout_msec: 100,
            },
            sink_writer: SpringSinkWriterConfig {
                net_connect_timeout_msec: 1000,
                net_write_timeout_msec: 100,
            },
        }
    }
}

/// Config related to worker threads.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct SpringWorkerConfig {
    pub n_generic_worker_threads: u16,
    pub n_source_worker_threads: u16,
}

/// Config related to memory management.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
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
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SpringWebConsoleConfig {
    pub enable_report_post: bool,

    pub report_interval_msec: u32,

    pub host: String,
    pub port: u16,

    pub timeout_msec: u32,
}

/// Config related to source reader
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct SpringSourceReaderConfig {
    pub net_connect_timeout_msec: u32,
    pub net_read_timeout_msec: u32,

    pub can_read_timeout_msec: u32,
}

/// Config related to sink writer.
#[allow(missing_docs)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct SpringSinkWriterConfig {
    pub net_connect_timeout_msec: u32,
    pub net_write_timeout_msec: u32,
}
