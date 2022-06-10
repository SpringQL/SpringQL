// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
};

use crate::stream_engine::Row;

#[derive(Debug, Default)]
pub(in crate::stream_engine) struct InMemoryQueue(
    Mutex<VecDeque<Row>>, // TODO faster (lock-free?) queue
);

impl InMemoryQueue {
    /// # Returns
    ///
    /// - `Ok(Some)` when at least a row is in the queue.
    /// - `None` when no row is in the queue.
    pub(in crate::stream_engine) fn pop_non_blocking(&self) -> Option<Row> {
        self.lock().pop_front()
    }

    pub(in crate::stream_engine) fn push(&self, row: Row) {
        self.lock().push_back(row)
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<Row>> {
        self.0
            .lock()
            .expect("another thread sharing the same InMemoryQueue internal got panic")
    }
}
