// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{collections::VecDeque, sync::Mutex};

use crate::stream_engine::autonomous_executor::row::StreamRow;

/// Input queue of window tasks.
///
/// Window queue has complicated structure, compared to row queue.
///
/// ![Window queue](https://raw.githubusercontent.com/SpringQL/SpringQL/main/springql-core/doc/img/window-queue.drawio.svg)
#[derive(Debug, Default)]
pub struct WindowQueue {
    waiting_q: Mutex<VecDeque<StreamRow>>,
}

impl WindowQueue {
    pub fn put(&self, row: StreamRow) {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .push_back(row);
    }

    pub fn dispatch(&self) -> Option<StreamRow> {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .pop_front()
    }

    pub fn purge(&self) {
        self.waiting_q
            .lock()
            .expect("mutex in WindowQueue is poisoned")
            .clear()
    }
}
