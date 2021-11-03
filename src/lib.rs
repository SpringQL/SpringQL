//! libSpringQL implementation.

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod model;
pub(crate) mod stream_engine;

mod api;

pub use api::*;

use once_cell::sync::Lazy;
use std::sync::Mutex;

static PIPELINE_CREATED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

#[cfg(test)]
mod test_support;
