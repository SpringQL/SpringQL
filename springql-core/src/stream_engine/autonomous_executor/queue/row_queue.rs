// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{collections::VecDeque, sync::Mutex};

use crate::stream_engine::autonomous_executor::row::Row;

/// Input queue of row tasks.
///
/// Just a FIFO buffer.
///
/// ![Row queue](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/row-queue.svg)
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

    pub(in crate::stream_engine::autonomous_executor) fn purge(&self) {
        self.q
            .lock()
            .expect("mutex in RowQueue is poisoned")
            .clear()
    }
}
