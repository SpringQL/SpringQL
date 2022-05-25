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

/// Pipeline
pub type SpringPipeline = pipeline::SpringPipeline;
pub use api::high_level_rs::SpringRow;

// re-export for low level api

/// configrations
pub mod config;
