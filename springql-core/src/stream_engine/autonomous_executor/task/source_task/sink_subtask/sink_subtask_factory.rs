// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::{option::Options, server_model::server_type::ServerType};

use super::in_memory_queue::InMemoryQueueSinkServerInstance;
use super::net::NetSinkServerInstance;
use super::SinkWriterInstance;

pub(in crate::stream_engine) struct SinkSubtaskFactory;

impl SinkSubtaskFactory {
    pub(in crate::stream_engine) fn sink(
        server_type: &ServerType,
        options: &Options,
    ) -> Result<Box<dyn SinkWriterInstance>> {
        match server_type {
            ServerType::SinkNet => {
                let server = NetSinkServerInstance::start(options)?;
                Ok(Box::new(server) as Box<dyn SinkWriterInstance>)
            }
            ServerType::SinkInMemoryQueue => {
                let server = InMemoryQueueSinkServerInstance::start(options)?;
                Ok(Box::new(server) as Box<dyn SinkWriterInstance>)
            }

            ServerType::SourceNet => unreachable!(),
        }
    }
}
