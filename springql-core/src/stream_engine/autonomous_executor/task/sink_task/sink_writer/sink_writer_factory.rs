// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::low_level_rs::SpringSinkWriterConfig;
use crate::pipeline::option::Options;
use crate::pipeline::sink_writer_model::sink_writer_type::SinkWriterType;

use super::in_memory_queue::InMemoryQueueSinkWriter;
use super::net::NetSinkWriter;
use super::SinkWriter;

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
