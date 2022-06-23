// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
};

use crate::stream_engine::StreamRow;

#[derive(Debug, Default)]
pub struct InMemoryQueue(
    Mutex<VecDeque<StreamRow>>, // TODO faster (lock-free?) queue
);

impl InMemoryQueue {
    /// # Returns
    ///
    /// - `Ok(Some)` when at least a row is in the queue.
    /// - `None` when no row is in the queue.
    pub fn pop_non_blocking(&self) -> Option<StreamRow> {
        self.lock().pop_front()
    }

    pub fn push(&self, row: StreamRow) {
        self.lock().push_back(row)
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<StreamRow>> {
        self.0
            .lock()
            .expect("another thread sharing the same InMemoryQueue internal got panic")
    }
}
