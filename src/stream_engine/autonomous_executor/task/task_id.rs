use std::fmt::Display;

use crate::model::name::{PumpName, StreamName};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct TaskId(String);

impl TaskId {
    pub(in crate::stream_engine) fn from_pump(pump: PumpName) -> Self {
        Self(format!("task-Pump-{}", pump))
    }
    pub(in crate::stream_engine) fn from_source_server(outgoing_stream: StreamName) -> Self {
        Self(format!("task-SourceServerTo-{}", outgoing_stream))
    }
    pub(in crate::stream_engine) fn from_sink_server(incoming_stream: StreamName) -> Self {
        Self(format!("task-SinkServerFrom-{}", incoming_stream))
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
