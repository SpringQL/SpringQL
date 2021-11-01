//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! A TaskGraph has "virtual root stream", who has outgoing edges to all source foreign streams.
//!
//! Tasks are mapped to TaskGraph's edges. StreamName's are mapped to its nodes.

use petgraph::graph::{DiGraph, NodeIndex};

use crate::{
    error::{Result, SpringError},
    model::name::StreamName,
    stream_engine::{
        autonomous_executor::task::pump_task::PumpTask,
        pipeline::pipeline_graph::{edge::Edge, PipelineGraph},
    },
};

use super::{sink_task::SinkTask, source_task::SourceTask, Task};

#[derive(Debug)]
pub(in crate::stream_engine) struct TaskGraph(DiGraph<StreamName, Task>);

impl From<&PipelineGraph> for TaskGraph {
    fn from(p_graph: &PipelineGraph) -> Self {
        let p_graph = p_graph.as_petgraph();
        let t_graph = p_graph.map(
            |_, stream_node| stream_node.name(),
            |_, edge| match edge {
                Edge::Pump(pump) => Task::Pump(PumpTask::from(pump)),
                Edge::Source(server) => {
                    Task::Source(SourceTask::new(server).expect("TODO make this panic-free"))
                }
                Edge::Sink(server) => Task::Sink(SinkTask::from(server)),
            },
        );
        Self(t_graph)
    }
}

impl TaskGraph {
    pub(in crate::stream_engine) fn as_petgraph(&self) -> &DiGraph<StreamName, Task> {
        &self.0
    }
}
