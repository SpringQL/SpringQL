// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::pipeline::name::SinkWriterName;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum SinkWriterType {
    Net,
    InMemoryQueue,
}

impl From<&SinkWriterType> for SinkWriterName {
    fn from(sink_writer_type: &SinkWriterType) -> Self {
        match sink_writer_type {
            SinkWriterType::Net => SinkWriterName::net_sink(),
            SinkWriterType::InMemoryQueue => SinkWriterName::in_memory_queue_sink(),
        }
    }
}
