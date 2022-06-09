// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::fmt::Display;

use crate::pipeline::{
    pipeline_graph::Edge,
    pump_model::{PumpInputType, PumpModel},
    sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum TaskId {
    Source {
        id: String,
    },
    Pump {
        id: String,
        input_type: PumpInputType,
    },
    Sink {
        id: String,
    },
}

impl TaskId {}

impl TaskId {
    pub(in crate::stream_engine::autonomous_executor) fn from_pump(pump: &PumpModel) -> Self {
        let id = pump.name().to_string();
        Self::Pump {
            id,
            input_type: pump.input_type(),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn from_source(
        source: &SourceReaderModel,
    ) -> Self {
        let id = source.name().to_string();
        Self::Source { id }
    }

    pub(in crate::stream_engine::autonomous_executor) fn from_sink(sink: &SinkWriterModel) -> Self {
        let id = sink.name().to_string();
        Self::Sink { id }
    }

    pub(in crate::stream_engine::autonomous_executor) fn is_window_task(&self) -> bool {
        match self {
            TaskId::Pump { input_type, .. } => *input_type == PumpInputType::Window,
            _ => false,
        }
    }
}

impl From<&Edge> for TaskId {
    fn from(edge: &Edge) -> Self {
        match edge {
            Edge::Pump { pump_model, .. } => TaskId::from_pump(pump_model.as_ref()),
            Edge::Source(source) => TaskId::from_source(source),
            Edge::Sink(sink) => TaskId::from_sink(sink),
        }
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id = match self {
            TaskId::Source { id } => id,
            TaskId::Pump { id, .. } => id,
            TaskId::Sink { id } => id,
        };
        write!(f, "{}", id)
    }
}
