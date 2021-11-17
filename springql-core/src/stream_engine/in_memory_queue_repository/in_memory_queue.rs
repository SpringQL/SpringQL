use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
    thread,
    time::Duration,
};

use crate::stream_engine::autonomous_executor::row::foreign_row::foreign_sink_row::ForeignSinkRow;

/// TODO config
const SLEEP_MSECS: u64 = 10;

#[derive(Debug, Default)]
pub(in crate::stream_engine) struct InMemoryQueue(
    Mutex<VecDeque<ForeignSinkRow>>, // TODO faster (lock-free?) queue
);

impl InMemoryQueue {
    /// Blocking call    
    pub(in crate::stream_engine) fn pop(&self) -> ForeignSinkRow {
        loop {
            let r = self.lock().pop_front();
            if let Some(r) = r {
                return r;
            } else {
                thread::sleep(Duration::from_millis(SLEEP_MSECS));
            }
        }
    }

    pub(in crate::stream_engine) fn push(&self, row: ForeignSinkRow) {
        self.lock().push_back(row)
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<ForeignSinkRow>> {
        self.0
            .lock()
            .expect("another thread sharing the same InMemoryQueue internal got panic")
    }
}
