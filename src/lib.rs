//! libSpringQL implementation.

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

static PIPELINE_CREATED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

pub(crate) mod model;
pub(crate) mod stream_engine;

mod api;

use std::sync::Mutex;

pub use api::*;
use once_cell::sync::Lazy;
