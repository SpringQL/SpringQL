// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! C API and high-level Rust API are provided separately.

pub use crate::config::{
    spring_config_default, spring_config_toml, SpringConfig, SpringMemoryConfig,
    SpringSinkWriterConfig, SpringSourceReaderConfig, SpringWebConsoleConfig, SpringWorkerConfig,
};

pub use crate::pipeline::{
    spring_column_bool, spring_column_f32, spring_column_i16, spring_column_i32, spring_column_i64,
    spring_column_text, spring_command, spring_open, spring_pop, spring_pop_non_blocking,
    SpringPipeline,
};
