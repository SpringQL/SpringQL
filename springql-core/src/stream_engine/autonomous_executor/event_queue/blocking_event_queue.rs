// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Mutex, MutexGuard},
};

use bus::{Bus, BusReader};

use super::event::{Event, EventTag};

/// Event queue (message broker) for Choreography-based Saga pattern.
#[derive(Default)]
pub(in crate::stream_engine::autonomous_executor) struct BlockingEventQueue {
    bus_by_tag: Mutex<HashMap<EventTag, Bus<Event>>>,
}

impl BlockingEventQueue {
    /// Publish an event to queue (blocking). This function returns after all subscribers subscribe the event.
    pub(in crate::stream_engine::autonomous_executor) fn publish_blocking(&self, event: Event) {
        let tag = EventTag::from(&event);

        let mut bus_by_tag = self.lock();
        let opt_bus = bus_by_tag.get_mut(&tag);

        if let Some(bus) = opt_bus {
            bus.broadcast(event)
        }
    }

    /// Subscribe to an event tag and get event polling target.
    ///
    /// A worker need to call this method just 1 time if it needs an event tag.
    pub(in crate::stream_engine::autonomous_executor) fn subscribe(
        &self,
        tag: EventTag,
    ) -> BlockingEventPoll {
        let mut bus_by_tag = self.lock();

        let bus_rx = match bus_by_tag.entry(tag) {
            Entry::Occupied(mut bus) => bus.get_mut().add_rx(),
            Entry::Vacant(v) => {
                let mut bus = Bus::new(100);
                let rx = bus.add_rx();
                v.insert(bus);
                rx
            }
        };

        BlockingEventPoll::new(bus_rx)
    }

    fn lock(&self) -> MutexGuard<HashMap<EventTag, Bus<Event>>> {
        self.bus_by_tag.lock().expect("EventQueue lock poisoned")
    }
}

#[derive(new)]
pub(in crate::stream_engine::autonomous_executor) struct BlockingEventPoll {
    rx: BusReader<Event>,
}

impl BlockingEventPoll {
    /// Blocking call
    pub(in crate::stream_engine::autonomous_executor) fn poll(&mut self) -> Event {
        self.rx
            .recv()
            .expect("Bus in BlockingEventQueue has dropped")
    }
}
