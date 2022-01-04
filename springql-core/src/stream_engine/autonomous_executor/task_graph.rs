// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Task graph is most important runtime plan for task execution.
//! Tasks are mapped to TaskGraph's nodes. queues are mapped to its nodes.
//!
//! A TaskGraph is uniquely generated from a Pipeline.

pub(super) mod queue_id;
pub(super) mod task_id;

pub(super) mod task_position;

mod edge_ref;

use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};

use self::{
    edge_ref::MyEdgeRef,
    queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId, QueueId},
    task_id::TaskId,
    task_position::TaskPosition,
};

#[derive(Debug, Default)]
pub(super) struct TaskGraph {
    g: DiGraph<TaskId, QueueId>,
    task_id_node_map: HashMap<TaskId, NodeIndex>,
    queue_id_edge_map: HashMap<QueueId, MyEdgeRef>,
}

impl TaskGraph {
    pub(super) fn task_position(&self, task_id: &TaskId) -> TaskPosition {
        let node = self.find_node(task_id);
        let has_incoming = self
            .g
            .neighbors_directed(node, petgraph::Direction::Incoming)
            .next()
            .is_some();
        let has_outgoing = self
            .g
            .neighbors_directed(node, petgraph::Direction::Outgoing)
            .next()
            .is_some();

        match (has_incoming, has_outgoing) {
            (true, true) => TaskPosition::Intermediate,
            (true, false) => TaskPosition::Sink,
            (false, true) => TaskPosition::Source,
            _ => unreachable!(),
        }
    }

    pub(super) fn upstream_task(&self, queue_id: &QueueId) -> TaskId {
        let edge = self.find_edge(queue_id);
        let source = edge.source();
        self.g
            .node_weight(source)
            .expect("must be valid index")
            .clone()
    }

    pub(super) fn downstream_task(&self, queue_id: &QueueId) -> TaskId {
        let edge = self.find_edge(queue_id);
        let target = edge.target();
        self.g
            .node_weight(target)
            .expect("must be valid index")
            .clone()
    }

    pub(super) fn input_queues(&self, task_id: &TaskId) -> Vec<QueueId> {
        let i = self.find_node(task_id);
        self.g
            .edges_directed(i, petgraph::EdgeDirection::Incoming)
            .into_iter()
            .map(|e| e.weight())
            .cloned()
            .collect()
    }
    fn output_queues(&self, task_id: &TaskId) -> Vec<QueueId> {
        let i = self.find_node(task_id);
        self.g
            .edges_directed(i, petgraph::EdgeDirection::Outgoing)
            .into_iter()
            .map(|e| e.weight())
            .cloned()
            .collect()
    }

    pub(super) fn downstream_tasks(&self, task_id: &TaskId) -> Vec<TaskId> {
        self.output_queues(task_id)
            .iter()
            .map(|q| self.downstream_task(q))
            .collect()
    }

    pub(super) fn tasks(&self) -> Vec<TaskId> {
        self.g.node_weights().cloned().collect()
    }

    pub(super) fn source_tasks(&self) -> Vec<TaskId> {
        self.tasks()
            .iter()
            .filter(|t| self.task_position(t) == TaskPosition::Source)
            .cloned()
            .collect()
    }

    pub(super) fn sink_tasks(&self) -> Vec<TaskId> {
        self.tasks()
            .iter()
            .filter(|t| self.task_position(t) == TaskPosition::Sink)
            .cloned()
            .collect()
    }

    pub(super) fn window_tasks(&self) -> Vec<TaskId> {
        self.tasks()
            .iter()
            .filter(|t| matches!(t, TaskId::Window(..)))
            .cloned()
            .collect()
    }

    pub(super) fn row_queues(&self) -> Vec<RowQueueId> {
        self.g
            .edge_weights()
            .filter_map(|queue_id| {
                if let QueueId::Row(id) = queue_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn window_queues(&self) -> Vec<WindowQueueId> {
        self.g
            .edge_weights()
            .filter_map(|queue_id| {
                if let QueueId::Window(id) = queue_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn add_task(&mut self, task_id: TaskId) {
        let i = self.g.add_node(task_id.clone());
        let _ = self.task_id_node_map.insert(task_id, i);
    }

    /// # Panics
    ///
    /// `source` or `target` task is not added in the graph.
    pub(super) fn add_queue(&mut self, queue_id: QueueId, source: TaskId, target: TaskId) {
        let source = self.find_node(&source);
        let target = self.find_node(&target);
        let i = self.g.add_edge(source, target, queue_id.clone());

        let edge_ref = MyEdgeRef::new(source, target, i, queue_id.clone());
        let _ = self.queue_id_edge_map.insert(queue_id, edge_ref);
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
