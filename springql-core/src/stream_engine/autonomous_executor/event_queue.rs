// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! This mod provides event queue mechanism.
//!
//! To add/del events, modify `self::event` module.

pub(in crate::stream_engine::autonomous_executor) mod event;
pub(in crate::stream_engine::autonomous_executor) mod non_blocking_event_queue;
