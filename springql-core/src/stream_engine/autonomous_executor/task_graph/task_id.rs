use std::fmt::Display;

use crate::pipeline::{
    pipeline_graph::edge::Edge,
    pump_model::{pump_input_type::PumpInputType, PumpModel},
    sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

use self::{row_task_id::RowTaskId, window_task_id::WindowTaskId};
use serde::{Deserialize, Serialize};

pub(in crate::stream_engine::autonomous_executor) mod row_task_id;
pub(in crate::stream_engine::autonomous_executor) mod window_task_id;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine::autonomous_executor) enum TaskId {
    /// Source and sink tasks are included in row task.
    Row(RowTaskId),
    Window(WindowTaskId),
}

impl TaskId {
    pub(in crate::stream_engine::autonomous_executor) fn from_pump(pump: &PumpModel) -> Self {
        let name = pump.name().to_string();
        match pump.input_type() {
            PumpInputType::Row => Self::Row(RowTaskId::new(name)),
            PumpInputType::Window => Self::Window(WindowTaskId::new(name)),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn from_source(
        source: &SourceReaderModel,
    ) -> Self {
        let name = source.name().to_string();
        Self::Row(RowTaskId::new(name))
    }

    pub(in crate::stream_engine::autonomous_executor) fn from_sink(sink: &SinkWriterModel) -> Self {
        let name = sink.name().to_string();
        Self::Row(RowTaskId::new(name))
    }
}

impl From<RowTaskId> for TaskId {
    fn from(row_task_id: RowTaskId) -> Self {
        Self::Row(row_task_id)
    }
}
impl From<WindowTaskId> for TaskId {
    fn from(window_task_id: WindowTaskId) -> Self {
        Self::Window(window_task_id)
    }
}

impl From<&Edge> for TaskId {
    fn from(edge: &Edge) -> Self {
        match edge {
            Edge::Pump(pump) => TaskId::from_pump(pump.as_ref()),
            Edge::Source(source) => TaskId::from_source(source),
            Edge::Sink(sink) => TaskId::from_sink(sink),
        }
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskId::Row(t) => write!(f, "{}", t),
            TaskId::Window(t) => write!(f, "{}", t),
        }
    }
}
