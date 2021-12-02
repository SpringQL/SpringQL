// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine) mod child_direction;
pub(crate) mod query_plan_operation;

mod graph_eq;

use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};

use crate::pipeline::name::StreamName;

use self::{child_direction::ChildDirection, query_plan_operation::QueryPlanOperation};

/// Query plan from which an executor can do its work deterministically.
///
/// This is a binary tree because every SELECT operation can break down into unary or binary operations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct QueryPlan(DiGraph<QueryPlanOperation, ChildDirection>);

impl PartialEq for QueryPlan {
    fn eq(&self, other: &Self) -> bool {
        graph_eq::graph_eq(&self.0, &other.0)
    }
}
impl Eq for QueryPlan {}

impl QueryPlan {
    pub(crate) fn add_root(&mut self, op: QueryPlanOperation) {
        self.0.add_node(op);
    }

    /// # Panics
    ///
    /// `parent_op` does not exist in tree
    pub(crate) fn add_left(&mut self, parent_op: &QueryPlanOperation, left_op: QueryPlanOperation) {
        self.add_child(parent_op, left_op, ChildDirection::Left);
    }

    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        self.0
            .node_weights()
            .filter_map(|op| match op {
                QueryPlanOperation::Collect { stream } => Some(stream),
                _ => None,
            })
            .collect()
    }

    pub(in crate::stream_engine) fn as_petgraph(
        &self,
    ) -> &DiGraph<QueryPlanOperation, ChildDirection> {
        &self.0
    }

    fn add_child(
        &mut self,
        parent_op: &QueryPlanOperation,
        child_op: QueryPlanOperation,
        lr: ChildDirection,
    ) {
        let child_node = self.0.add_node(child_op);
        let parent_node = self
            .0
            .node_indices()
            .find(|i| {
                self.0
                    .node_weight(*i)
                    .expect("parent does not exist in tree")
                    == parent_op
            })
            .unwrap();

        self.0.add_edge(parent_node, child_node, lr);
    }
}
