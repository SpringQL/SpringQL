// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::{option::Options, server_model::server_type::ServerType};
use crate::stream_engine::autonomous_executor::task::source_task::source_subtask::SourceSubtask;

use super::net::NetSourceSubtask;

pub(in crate::stream_engine::autonomous_executor) struct SourceSubtaskFactory;

impl SourceSubtaskFactory {
    pub(in crate::stream_engine::autonomous_executor) fn source(
        server_type: &ServerType,
        options: &Options,
    ) -> Result<Box<dyn SourceSubtask>> {
        let server = match server_type {
            ServerType::SourceNet => NetSourceSubtask::start(options),

            ServerType::SinkNet | ServerType::SinkInMemoryQueue => unreachable!(),
        }?;
        Ok(Box::new(server))
    }
}
