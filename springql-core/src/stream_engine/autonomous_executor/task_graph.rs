// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Task graph is most important runtime plan for task execution.
//! Tasks are mapped to TaskGraph's nodes. queues are mapped to its nodes.
//!
//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! # Task graph concept diagram
//!
//! ![Task graph concept diagram](https://raw.githubusercontent.com/SpringQL/SpringQL/main/springql-core/doc/img/pipeline-and-task-graph.drawio.svg)

mod queue_id;
mod task_id;

mod edge_ref;

pub use queue_id::{QueueId, RowQueueId, WindowQueueId};
pub use task_id::TaskId;

use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};

use crate::{
    pipeline::{Edge, Pipeline, PipelineVersion, StreamName},
    stream_engine::autonomous_executor::task_graph::edge_ref::MyEdgeRef,
};

#[derive(Clone, Debug, new)]
pub struct QueueIdWithUpstream {
    queue_id: QueueId,
    upstream: StreamName, // FIXME avoid mixing pipeline and task graph objects. It leads to [#86](https://github.com/SpringQL/SpringQL/pull/86)
}

#[derive(Debug)]
pub struct TaskGraph {
    /// From which version this graph constructed
    pipeline_version: PipelineVersion,

    g: DiGraph<TaskId, QueueIdWithUpstream>,
    task_id_node_map: HashMap<TaskId, NodeIndex>,
    queue_id_edge_map: HashMap<QueueId, MyEdgeRef>,
}

impl TaskGraph {
    pub fn new(pipeline_version: PipelineVersion) -> Self {
        Self {
            pipeline_version,
            g: DiGraph::default(),
            task_id_node_map: HashMap::default(),
            queue_id_edge_map: HashMap::default(),
        }
    }

    pub fn pipeline_version(&self) -> &PipelineVersion {
        &self.pipeline_version
    }

    pub fn upstream_task(&self, queue_id: &QueueId) -> TaskId {
        let edge = self.find_edge(queue_id);
        let source = edge.source();
        self.g
            .node_weight(source)
            .expect("must be valid index")
            .clone()
    }

    pub fn downstream_task(&self, queue_id: &QueueId) -> TaskId {
        let edge = self.find_edge(queue_id);
        let target = edge.target();
        self.g
            .node_weight(target)
            .expect("must be valid index")
            .clone()
    }

    pub fn input_queues(&self, task_id: &TaskId) -> Vec<QueueId> {
        let i = self.find_node(task_id);
        self.g
            .edges_directed(i, petgraph::EdgeDirection::Incoming)
            .map(|e| &e.weight().queue_id)
            .cloned()
            .collect()
    }
    pub fn output_queues(&self, task_id: &TaskId) -> Vec<QueueId> {
        let i = self.find_node(task_id);
        self.g
            .edges_directed(i, petgraph::EdgeDirection::Outgoing)
            .map(|e| &e.weight().queue_id)
            .cloned()
            .collect()
    }

    /// # Returns
    ///
    /// `None` if `task_id` does not have incoming edge (queue) from `upstream`.
    /// This may happen when `task_id` and `upstream` come from different versions of pipeline.
    pub fn input_queue(&self, task_id: &TaskId, upstream: &StreamName) -> Option<QueueId> {
        let i = self.find_node(task_id);
        self.g
            .edges_directed(i, petgraph::EdgeDirection::Incoming)
            .find_map(|e| {
                let queue_id_with_upstream = e.weight();
                (&queue_id_with_upstream.upstream == upstream)
                    .then(|| queue_id_with_upstream.queue_id.clone())
            })
    }

    pub fn downstream_tasks(&self, task_id: &TaskId) -> Vec<TaskId> {
        self.output_queues(task_id)
            .iter()
            .map(|q| self.downstream_task(q))
            .collect()
    }

    pub fn tasks(&self) -> Vec<TaskId> {
        self.g.node_weights().cloned().collect()
    }

    pub fn source_tasks(&self) -> Vec<TaskId> {
        self.tasks()
            .iter()
            .filter(|t| matches!(t, TaskId::Source { .. }))
            .cloned()
            .collect()
    }

    pub fn window_tasks(&self) -> Vec<TaskId> {
        self.tasks()
            .iter()
            .filter(|t| t.is_window_task())
            .cloned()
            .collect()
    }

    pub fn row_queues(&self) -> Vec<RowQueueId> {
        self.g
            .edge_weights()
            .filter_map(|queue_id| {
                if let QueueId::Row(id) = queue_id.queue_id.clone() {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn window_queues(&self) -> Vec<WindowQueueId> {
        self.g
            .edge_weights()
            .filter_map(|queue_id| {
                if let QueueId::Window(id) = queue_id.queue_id.clone() {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn add_task(&mut self, task_id: TaskId) {
        let i = self.g.add_node(task_id.clone());
        let _ = self.task_id_node_map.insert(task_id, i);
    }

    /// # Panics
    ///
    /// `source` or `target` task is not added in the graph.
    pub fn add_queue(&mut self, queue_id: QueueIdWithUpstream, source: TaskId, target: TaskId) {
        let source = self.find_node(&source);
        let target = self.find_node(&target);
        let _ = self.g.add_edge(source, target, queue_id.clone());

        let edge_ref = MyEdgeRef::new(source, target);
        let _ = self.queue_id_edge_map.insert(queue_id.queue_id, edge_ref);
    }

    /// # Panics
    ///
    /// `task_id` is not added in the graph.
    fn find_node(&self, task_id: &TaskId) -> NodeIndex {
        *self
            .task_id_node_map
            .get(task_id)
            .unwrap_or_else(|| panic!(r#"task {:?} is not added in the graph"#, task_id))
    }

    /// # Panics
    ///
    /// `queue_id` is not added in the graph.
    fn find_edge(&self, queue_id: &QueueId) -> MyEdgeRef {
        self.queue_id_edge_map
            .get(queue_id)
            .unwrap_or_else(|| panic!(r#"queue {:?} is not added in the graph"#, queue_id))
            .clone()
    }
}

impl From<&Pipeline> for TaskGraph {
    fn from(pipeline: &Pipeline) -> Self {
        let pipeline_graph = pipeline.as_graph();
        let pipeline_petgraph = pipeline_graph.as_petgraph();
        let mut task_graph = TaskGraph::new(pipeline.version());

        // add all task ids
        pipeline_petgraph.edge_weights().for_each(|edge| {
            let task_id = TaskId::from(edge);
            // duplicate task id on JOIN pump task (but it's ok)
            task_graph.add_task(task_id);
        });

        // Add all queues.
        for edge_ref in pipeline_petgraph.edge_references() {
            match edge_ref.weight() {
                Edge::Pump {
                    pump_model,
                    upstream,
                } => {
                    let queue_id = QueueId::from_pump(pump_model, upstream);
                    let target = TaskId::from_pump(pump_model);
                    pipeline_graph
                        .upstream_edges(&edge_ref)
                        .iter()
                        .for_each(|source_edge_ref| {
                            let source_edge = source_edge_ref.weight();
                            let source = TaskId::from(source_edge);
                            task_graph.add_queue(
                                QueueIdWithUpstream::new(queue_id.clone(), upstream.clone()),
                                source,
                                target.clone(),
                            );
                        })
                }
                Edge::Sink(sink) => {
                    let queue_id = QueueId::from_sink(sink);
                    let target = TaskId::from_sink(sink);
                    let source_edge = pipeline_graph
                        .upstream_edges(&edge_ref)
                        .first()
                        .expect("sink writer must have 1 upstream pump")
                        .weight();
                    let source = TaskId::from(source_edge);
                    task_graph.add_queue(
                        QueueIdWithUpstream::new(queue_id, sink.sink_upstream().clone()),
                        source,
                        target,
                    );
                }
                Edge::Source(_) => {} // no queue is created for source task
            };
        }
        task_graph
    }
}
