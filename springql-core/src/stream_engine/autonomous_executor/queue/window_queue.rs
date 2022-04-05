// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{collections::VecDeque, sync::Mutex};

use crate::stream_engine::autonomous_executor::row::Row;

/// Input queue of window tasks.
///
/// Window queue has complicated structure, compared to row queue.
///
/// ![Window queue](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/window-queue.svg)
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct WindowQueue {
    waiting_q: Mutex<VecDeque<Row>>,
}

impl WindowQueue {
    pub(in crate::stream_engine::autonomous_executor) fn put(&self, row: Row) {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .push_back(row);
    }

    pub(in crate::stream_engine::autonomous_executor) fn dispatch(&self) -> Option<Row> {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .pop_front()
    }

    pub(in crate::stream_engine::autonomous_executor) fn purge(&self) {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .clear()
    }
}
