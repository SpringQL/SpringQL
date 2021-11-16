use std::{collections::VecDeque, sync::Mutex};

use crate::stream_engine::autonomous_executor::row::foreign_row::foreign_sink_row::ForeignSinkRow;

#[derive(Debug, Default)]
pub(in crate::stream_engine) struct InMemoryQueue(
    Mutex<VecDeque<ForeignSinkRow>>, // TODO faster (lock-free?) queue
);

impl InMemoryQueue {
    pub(in crate::stream_engine) fn pop(&self) -> ForeignSinkRow {
        todo!()
    }
}
