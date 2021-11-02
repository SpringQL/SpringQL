use crate::model::name::{PumpName, StreamName};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(in crate::stream_engine) struct TaskId(String);

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

impl ToString for TaskId {
    fn to_string(&self) -> String {
        self.0.clone()
    }
}
