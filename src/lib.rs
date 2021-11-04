//! libSpringQL implementation.

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod pipeline;
pub(crate) mod sql_parser;
pub(crate) mod stream_engine;

mod api;

#[cfg(test)]
mod test_support;

pub use api::*;

use once_cell::sync::Lazy;
use std::sync::atomic::AtomicBool;

static PIPELINE_CREATED: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
