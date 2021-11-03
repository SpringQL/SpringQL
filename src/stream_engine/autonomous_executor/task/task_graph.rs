//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! A TaskGraph has "virtual root stream", who has outgoing edges to all source foreign streams.
//!
//! Tasks are mapped to TaskGraph's edges. StreamName's are mapped to its nodes.

use std::sync::{Arc, RwLock};

use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::{EdgeRef, IntoEdgeReferences, IntoEdgesDirected},
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
    fn from(pipeline_graph: &PipelineGraph) -> Self {
        let p_graph = pipeline_graph.as_petgraph();
        let t_graph = p_graph.map(
            |_, stream_node| stream_node.name(),
            |_, edge| {
                let task = match edge {
                    Edge::Pump(pump) => Task::Pump(PumpTask::from(pump)),
                    Edge::Source(server) => Task::Source(SourceTask::new(server, pipeline_graph)),
                    Edge::Sink(server) => {
                        Task::Sink(SinkTask::new(server).expect("TODO make this panic-free"))
                    }
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
            .find_map(|t| (t.weight().id() == task).then(|| t.target()))
            .unwrap_or_else(|| panic!("task ({:?}) must be in TaskGraph:\n{:?}", task, self.0));
        let outgoing_edges = self
            .0
            .edges_directed(downstream_node, petgraph::Direction::Outgoing);
        outgoing_edges.map(|edge| edge.weight().id()).collect()
    }

    pub(in crate::stream_engine) fn all_tasks(&self) -> Vec<TaskId> {
        self.0.edge_weights().map(|t| t.id()).collect()
    }

    pub(in crate::stream_engine) fn as_petgraph(&self) -> &DiGraph<StreamName, Arc<Task>> {
        &self.0
    }
}
