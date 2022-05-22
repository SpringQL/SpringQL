// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! C API and high-level Rust API are provided separately.

pub(crate) mod engine_mutex {
    pub(crate) use crate::stream_engine::EngineMutex;
}
mod spring_config {

    pub use crate::config::{
        spring_config_default, spring_config_toml, SpringConfig, SpringMemoryConfig,
        SpringSinkWriterConfig, SpringSourceReaderConfig, SpringWebConsoleConfig,
        SpringWorkerConfig,
    };
}

pub use spring_config::*;

pub(crate) use self::engine_mutex::EngineMutex;

pub use crate::pipeline::spring_column_bool;
pub use crate::pipeline::spring_column_f32;
pub use crate::pipeline::spring_column_i16;
pub use crate::pipeline::spring_column_i32;
pub use crate::pipeline::spring_column_i64;
pub use crate::pipeline::spring_column_text;
pub use crate::pipeline::spring_command;
pub use crate::pipeline::spring_open;
pub use crate::pipeline::spring_pop;
pub use crate::pipeline::spring_pop_non_blocking;
pub use crate::pipeline::SpringPipeline;
