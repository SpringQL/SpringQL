// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("lib.md")]
#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod expr_resolver;
pub(crate) mod expression;
pub(crate) mod mem_size;
pub(crate) mod pipeline;
pub(crate) mod sql_processor;
pub(crate) mod stream_engine;
pub(crate) mod time;

mod api;

pub use api::*;
