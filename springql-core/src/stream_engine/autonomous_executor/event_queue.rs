// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! This mod provides event queue mechanism.
//!
//! To add/del events, modify `self::event` module.

pub mod blocking_event_queue;
pub mod event;
pub mod non_blocking_event_queue;

use std::sync::mpsc;

use crate::stream_engine::autonomous_executor::event_queue::event::Event;

#[derive(Debug, new)]
pub struct EventPoll {
    receiver: mpsc::Receiver<Event>,
}

impl EventPoll {
    /// Non-blocking call
    pub fn poll(&self) -> Option<Event> {
        self.receiver.try_recv().ok()
    }
}
