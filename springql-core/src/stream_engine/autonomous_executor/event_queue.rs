// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! This mod provides event queue mechanism.
//!
//! To add/del events, modify `self::event` module.

mod blocking_event_queue;
mod event;
mod non_blocking_event_queue;

pub use blocking_event_queue::BlockingEventQueue;
pub use event::{BlockingEventTag, Event, EventTag, NonBlockingEventTag};
pub use non_blocking_event_queue::NonBlockingEventQueue;

use std::sync::mpsc;

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
