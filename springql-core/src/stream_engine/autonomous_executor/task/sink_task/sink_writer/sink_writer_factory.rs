// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::{error::Result, SpringSinkWriterConfig},
    pipeline::{sink_writer_model::SinkWriterType, Options},
    stream_engine::autonomous_executor::task::sink_task::sink_writer::{
        in_memory_queue::InMemoryQueueSinkWriter, net::NetSinkWriter, SinkWriter,
    },
};

pub(in crate::stream_engine) struct SinkWriterFactory;

impl SinkWriterFactory {
    pub(in crate::stream_engine) fn sink(
        sink_writer_type: &SinkWriterType,
        options: &Options,
        config: &SpringSinkWriterConfig,
    ) -> Result<Box<dyn SinkWriter>> {
        match sink_writer_type {
            SinkWriterType::Net => {
                let sink_writer = NetSinkWriter::start(options, config)?;
                Ok(Box::new(sink_writer) as Box<dyn SinkWriter>)
            }
            SinkWriterType::InMemoryQueue => {
                let sink = InMemoryQueueSinkWriter::start(options, config)?;
                Ok(Box::new(sink) as Box<dyn SinkWriter>)
            }
        }
    }
}
