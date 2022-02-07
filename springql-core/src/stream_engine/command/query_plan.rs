// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod query_plan_operation;

pub(in crate::stream_engine) mod child_direction;

mod graph_eq;

use petgraph::graph::DiGraph;

use crate::pipeline::{name::StreamName, pump_model::pump_input_type::PumpInputType};

use self::{child_direction::ChildDirection, query_plan_operation::QueryPlanOperation};

/// Query plan from which an executor can do its work deterministically.
///
/// This is a binary tree because every SELECT operation can break down into unary or binary operations.
#[derive(Clone, Debug, Default)]
pub(crate) struct QueryPlan {
    tree: DiGraph<QueryPlanOperation, ChildDirection>,
}

impl PartialEq for QueryPlan {
    fn eq(&self, other: &Self) -> bool {
        graph_eq::graph_eq(&self.tree, &other.tree)
    }
}
impl Eq for QueryPlan {}

impl QueryPlan {
    pub(crate) fn input_type(&self) -> PumpInputType {
        // TODO distinguish window input
        PumpInputType::Row
    }

    pub(crate) fn add_root(&mut self, op: QueryPlanOperation) {
        self.tree.add_node(op);
    }

    /// # Panics
    ///
    /// `parent_op` does not exist in tree
    pub(crate) fn add_left(&mut self, parent_op: &QueryPlanOperation, left_op: QueryPlanOperation) {
        self.add_child(parent_op, left_op, ChildDirection::Left);
    }

    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        self.tree
            .node_weights()
            .filter_map(|op| match op {
                QueryPlanOperation::Collect { stream, .. } => Some(stream),
                _ => None,
            })
            .collect()
    }

    pub(in crate::stream_engine) fn as_petgraph(
        &self,
    ) -> &DiGraph<QueryPlanOperation, ChildDirection> {
        &self.tree
    }

    fn add_child(
        &mut self,
        parent_op: &QueryPlanOperation,
        child_op: QueryPlanOperation,
        lr: ChildDirection,
    ) {
        let child_node = self.tree.add_node(child_op);
        let parent_node = self
            .tree
            .node_indices()
            .find(|i| {
                self.tree
                    .node_weight(*i)
                    .expect("parent does not exist in tree")
                    == parent_op
            })
            .unwrap();

        self.tree.add_edge(parent_node, child_node, lr);
    }
}
