use std::{collections::VecDeque, sync::Mutex};

use crate::stream_engine::autonomous_executor::row::Row;

/// Input queue of row tasks.
///
/// Just a FIFO buffer.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct RowQueue {
    q: Mutex<VecDeque<Row>>,
}

impl RowQueue {
    pub(in crate::stream_engine::autonomous_executor) fn put(&self, row: Row) {
        self.q
            .lock()
            .expect("mutex in RowQueue is poisoned")
            .push_back(row);
    }

    pub(in crate::stream_engine::autonomous_executor) fn use_(&self) -> Option<Row> {
        self.q
            .lock()
            .expect("mutex in RowQueue is poisoned")
            .pop_front()
    }
}
