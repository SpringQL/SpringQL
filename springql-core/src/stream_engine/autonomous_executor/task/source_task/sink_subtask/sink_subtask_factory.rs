// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::{option::Options, server_model::server_type::ServerType};

use super::in_memory_queue::InMemoryQueueSinkSubtask;
use super::net::NetSinkSubtask;
use super::SinkSubtask;

pub(in crate::stream_engine) struct SinkSubtaskFactory;

impl SinkSubtaskFactory {
    pub(in crate::stream_engine) fn sink(
        server_type: &ServerType,
        options: &Options,
    ) -> Result<Box<dyn SinkSubtask>> {
        match server_type {
            ServerType::SinkNet => {
                let server = NetSinkSubtask::start(options)?;
                Ok(Box::new(server) as Box<dyn SinkSubtask>)
            }
            ServerType::SinkInMemoryQueue => {
                let server = InMemoryQueueSinkSubtask::start(options)?;
                Ok(Box::new(server) as Box<dyn SinkSubtask>)
            }

            ServerType::SourceNet => unreachable!(),
        }
    }
}
