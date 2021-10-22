//! libSpringQL implementation.

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub mod error;
pub mod timestamp;

pub(crate) mod stream_engine;

#[cfg(test)]
pub mod test_support;
