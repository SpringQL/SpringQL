// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::option::Options;
use crate::pipeline::sink_writer_model::sink_writer_type::SinkWriterType;

use super::in_memory_queue::InMemoryQueueSinkSubtask;
use super::net::NetSinkSubtask;
use super::SinkSubtask;

pub(in crate::stream_engine) struct SinkSubtaskFactory;

impl SinkSubtaskFactory {
    pub(in crate::stream_engine) fn sink(
        sink_writer_type: &SinkWriterType,
        options: &Options,
    ) -> Result<Box<dyn SinkSubtask>> {
        match sink_writer_type {
            SinkWriterType::Net => {
                let sink_subtask = NetSinkSubtask::start(options)?;
                Ok(Box::new(sink_subtask) as Box<dyn SinkSubtask>)
            }
            SinkWriterType::InMemoryQueue => {
                let sink = InMemoryQueueSinkSubtask::start(options)?;
                Ok(Box::new(sink) as Box<dyn SinkSubtask>)
            }
        }
    }
}
