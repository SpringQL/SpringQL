// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("lib.md")]
#![deny(missing_debug_implementations, missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[macro_use]
extern crate derive_new;

mod connection;
mod expr_resolver;
mod expression;
mod mem_size;
mod pipeline;
mod sql_processor;
mod stream_engine;
mod time;

/// public API for SpringQL
pub mod api;

#[cfg(not(feature = "stub_web_console"))]
mod http_blocking;

#[cfg(feature = "stub_web_console")]
mod stub_http_blocking;

#[cfg(feature = "stub_web_console")]
pub use stub_http_blocking::stubed_requests;
