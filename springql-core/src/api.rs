// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! The APIs exposed in this module are provided for other languages by the official client library.
//!
//! - [SpringQL C Client](https://github.com/SpringQL/SpringQL-client-c)
//!

pub mod error;
pub mod high_level_rs;
pub mod low_level_rs;

pub use crate::stream_engine::time::duration::event_duration::SpringEventDuration;
pub use crate::stream_engine::time::timestamp::SpringTimestamp;
pub use crate::stream_engine::SpringValue;
