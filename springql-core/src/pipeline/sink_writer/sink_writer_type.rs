// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::pipeline::name::SinkWriterName;

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum SinkWriterType {
    Net,
    InMemoryQueue,
}

impl From<&SinkWriterType> for SinkWriterName {
    fn from(server_type: &SinkWriterType) -> Self {
        match server_type {
            SinkWriterType::Net => SinkWriterName::net_sink(),
            SinkWriterType::InMemoryQueue => SinkWriterName::in_memory_queue_sink(),
        }
    }
}
