//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! A TaskGraph has "virtual root stream", who has outgoing edges to all source foreign streams.
//!
//! Tasks are mapped to TaskGraph's edges. StreamName's are mapped to its nodes.

use std::sync::Arc;

use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::{EdgeRef, IntoEdgesDirected},
};

use crate::{
    error::{Result, SpringError},
    model::name::StreamName,
    stream_engine::{
        autonomous_executor::task::pump_task::PumpTask,
        pipeline::pipeline_graph::{edge::Edge, PipelineGraph},
    },
};

use super::{sink_task::SinkTask, source_task::SourceTask, task_id::TaskId, Task};

#[derive(Debug, Default)]
pub(crate) struct TaskGraph(DiGraph<StreamName, Arc<Task>>);

impl From<&PipelineGraph> for TaskGraph {
    fn from(p_graph: &PipelineGraph) -> Self {
        let p_graph = p_graph.as_petgraph();
        let t_graph = p_graph.map(
            |_, stream_node| stream_node.name(),
            |_, edge| {
                let task = match edge {
                    Edge::Pump(pump) => Task::Pump(PumpTask::from(pump)),
                    Edge::Source(server) => {
                        Task::Source(SourceTask::new(server).expect("TODO make this panic-free"))
                    }
                    Edge::Sink(server) => Task::Sink(SinkTask::from(server)),
                };
                Arc::new(task)
            },
        );
        Self(t_graph)
    }
}

impl TaskGraph {
    /// # Panics
    ///
    /// `task` is not found in this TaskGraph
    pub(in crate::stream_engine) fn downstream_tasks(&self, task: TaskId) -> Vec<TaskId> {
        let downstream_node = self
            .0
            .edge_references()
            .find_map(|t| (t.weight().id() == &task).then(|| t.target()))
            .expect("task must be in TaskGraph");
        let outgoing_edges = self
            .0
            .edges_directed(downstream_node, petgraph::Direction::Outgoing);
        outgoing_edges
            .map(|edge| edge.weight().id())
            .cloned()
            .collect()
    }

    pub(in crate::stream_engine) fn as_petgraph(&self) -> &DiGraph<StreamName, Arc<Task>> {
        &self.0
    }
}
