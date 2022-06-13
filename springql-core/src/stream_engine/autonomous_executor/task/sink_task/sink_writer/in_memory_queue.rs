// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::{error::Result, SpringSinkWriterConfig},
    pipeline::{InMemoryQueueOptions, Options, QueueName},
    stream_engine::{
        autonomous_executor::task::sink_task::sink_writer::SinkWriter,
        in_memory_queue_repository::InMemoryQueueRepository, Row,
    },
};

#[derive(Debug)]
pub struct InMemoryQueueSinkWriter(QueueName);

impl SinkWriter for InMemoryQueueSinkWriter {
    fn start(options: &Options, _config: &SpringSinkWriterConfig) -> Result<Self>
    where
        Self: Sized,
    {
        let options = InMemoryQueueOptions::try_from(options)?;
        let queue_name = options.queue_name;
        InMemoryQueueRepository::instance().create(queue_name.clone())?;
        Ok(Self(queue_name))
    }

    fn send_row(&mut self, row: Row) -> Result<()> {
        let q = InMemoryQueueRepository::instance().get(&self.0)?;
        q.push(row);
        Ok(())
    }
}
