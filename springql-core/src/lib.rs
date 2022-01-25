// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! SpringQL-core implementation.
//!
//! # High-level architecture diagram
//!
//! ![High-level architecture diagram](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/springql-architecture.svg)

#![deny(missing_debug_implementations, missing_docs)]

#[macro_use]
extern crate derive_new;

pub(crate) mod expression;
pub(crate) mod mem_size;
pub(crate) mod pipeline;
pub(crate) mod sql_processor;
pub(crate) mod stream_engine;

mod api;

pub use api::*;
