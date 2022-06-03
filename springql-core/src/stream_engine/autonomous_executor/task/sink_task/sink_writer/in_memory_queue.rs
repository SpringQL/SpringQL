// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::{error::Result, SpringSinkWriterConfig},
    pipeline::{
        name::QueueName,
        option::{in_memory_queue_options::InMemoryQueueOptions, Options},
    },
    stream_engine::{
        autonomous_executor::{
            row::foreign_row::sink_row::SinkRow, task::sink_task::sink_writer::SinkWriter,
        },
        in_memory_queue_repository::InMemoryQueueRepository,
    },
};

#[derive(Debug)]
pub(in crate::stream_engine) struct InMemoryQueueSinkWriter(QueueName);

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

    fn send_row(&mut self, row: SinkRow) -> Result<()> {
        let q = InMemoryQueueRepository::instance().get(&self.0)?;
        q.push(row);
        Ok(())
    }
}
