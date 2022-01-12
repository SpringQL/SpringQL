//! This mod provides event queue mechanism.
//!
//! To add/del events, modify `self::event` module.

pub(in crate::stream_engine::autonomous_executor) mod event;

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{mpsc, Mutex, MutexGuard},
};

use self::event::{Event, EventTag};

/// Event queue (message broker) for Choreography-based Saga pattern.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct EventQueue {
    subscribers_by_tag: Mutex<HashMap<EventTag, Subscribers>>,
}

impl EventQueue {
    /// Publish an event to queue.
    ///
    /// Then queue will make the event ready for subscribers of the event tag.
    pub(in crate::stream_engine::autonomous_executor) fn publish(&self, event: Event) {
        let tag = EventTag::from(&event);

        let subscribers_by_tag = self.lock();
        let opt_subscribers = subscribers_by_tag.get(&tag);

        if let Some(subscribers) = opt_subscribers {
            subscribers.push_all(event)
        }
    }

    /// Subscribe to an event tag and get event polling target.
    ///
    /// A worker need to call this method just 1 time if it needs an event tag.
    pub(in crate::stream_engine::autonomous_executor) fn subscribe(
        &self,
        tag: EventTag,
    ) -> EventPoll {
        let (sender, receiver) = mpsc::channel();
        let event_push = EventPush::new(sender);
        let event_poll = EventPoll::new(receiver);

        let mut subscribers_by_tag = self.lock();

        // add new subscriber to self.subscribers
        match subscribers_by_tag.entry(tag) {
            Entry::Occupied(mut sub) => sub.get_mut().add(event_push),
            Entry::Vacant(v) => {
                let mut sub = Subscribers::default();
                sub.add(event_push);
                v.insert(sub);
            }
        }

        event_poll
    }

    fn lock(&self) -> MutexGuard<HashMap<EventTag, Subscribers>> {
        self.subscribers_by_tag
            .lock()
            .expect("EventQueue lock poisoned")
    }
}

#[derive(Debug, Default)]
struct Subscribers {
    event_push_list: Vec<EventPush>,
}

impl Subscribers {
    fn add(&mut self, event_push: EventPush) {
        self.event_push_list.push(event_push);
    }

    fn push_all(&self, event: Event) {
        for event_push in self.event_push_list.iter() {
            event_push.push(event.clone());
        }
    }
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct EventPoll {
    receiver: mpsc::Receiver<Event>,
}

impl EventPoll {
    /// Non-blocking call
    pub(in crate::stream_engine::autonomous_executor) fn poll(&self) -> Option<Event> {
        self.receiver.try_recv().ok()
    }
}

#[derive(Debug, new)]
struct EventPush {
    sender: mpsc::Sender<Event>,
}

impl EventPush {
    fn push(&self, event: Event) {
        self.sender
            .send(event)
            .expect("failed to send event to subscriber");
    }
}
