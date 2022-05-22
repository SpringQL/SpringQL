// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("lib.md")]
#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod expr_resolver;
pub(crate) mod expression;
pub(crate) mod mem_size;
pub mod pipeline;
pub(crate) mod sql_processor;
pub(crate) mod stream_engine;

mod api;

pub use api::*;

// re-export for high level api
pub use api::high_level_rs::SpringPipelineHL as SpringPipeline;
pub use api::high_level_rs::SpringRowHL as SpringRow;

// re-export for low level api

/// configrations
pub mod config {
    pub use crate::api::low_level_rs::SpringConfig;
    pub use crate::api::low_level_rs::SpringMemoryConfig;
    pub use crate::api::low_level_rs::SpringSinkWriterConfig;
    pub use crate::api::low_level_rs::SpringSourceReaderConfig;
    pub use crate::api::low_level_rs::SpringWebConsoleConfig;
    pub use crate::api::low_level_rs::SpringWorkerConfig;
}
