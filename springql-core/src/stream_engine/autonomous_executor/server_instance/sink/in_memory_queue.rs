use crate::error::Result;
use crate::{
    pipeline::option::Options,
    stream_engine::autonomous_executor::row::foreign_row::foreign_sink_row::ForeignSinkRow,
};
use std::collections::VecDeque;

use super::SinkServerInstance;

#[derive(Debug)]
pub(in crate::stream_engine) struct InMemoryQueueSinkServerInstance {
    queue: VecDeque<ForeignSinkRow>,
}

impl SinkServerInstance for InMemoryQueueSinkServerInstance {
    fn start(_options: &Options) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            queue: VecDeque::new(),
        })
    }

    fn send_row(&mut self, row: ForeignSinkRow) -> Result<()> {
        self.queue.push_front(row);
        Ok(())
    }
}
