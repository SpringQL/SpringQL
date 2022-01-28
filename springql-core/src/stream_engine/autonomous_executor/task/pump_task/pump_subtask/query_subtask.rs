// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::{EdgeRef, IntoNodeReferences},
};

use self::query_subtask_node::QuerySubtaskNode;
use crate::{
    error::Result,
    stream_engine::autonomous_executor::{
        performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByTaskExecution,
        task::{task_context::TaskContext, tuple::Tuple},
    },
    stream_engine::command::query_plan::{child_direction::ChildDirection, QueryPlan},
};

mod interm_row;
mod query_subtask_node;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtask {
    tree: DiGraph<QuerySubtaskNode, ChildDirection>,
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtaskOut {
    pub(in crate::stream_engine::autonomous_executor) tuple: Tuple,
    pub(in crate::stream_engine::autonomous_executor) in_queue_metrics_update:
        InQueueMetricsUpdateByTaskExecution,
}

impl From<&QueryPlan> for QuerySubtask {
    fn from(query_plan: &QueryPlan) -> Self {
        let plan_tree = query_plan.as_petgraph();
        let subtask_tree = plan_tree.map(
            |_, op| QuerySubtaskNode::from(op),
            |_, child_direction| child_direction.clone(),
        );
        Self { tree: subtask_tree }
    }
}

impl QuerySubtask {
    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    ///
    /// # Failures
    ///
    /// TODO
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<Option<QuerySubtaskOut>> {
        let mut next_idx = self.leaf_node_idx();

        match self.run_leaf(next_idx, context) {
            None => Ok(None),
            Some(leaf_query_subtask_out) => {
                let mut next_tuple = leaf_query_subtask_out.tuple;
                while let Some(parent_idx) = self.parent_node_idx(next_idx) {
                    next_idx = parent_idx;
                    next_tuple = self.run_non_leaf(next_idx, next_tuple)?;
                }

                Ok(Some(QuerySubtaskOut::new(
                    next_tuple,
                    leaf_query_subtask_out.in_queue_metrics_update, // leaf subtask decides in queue metrics change
                )))
            }
        }
    }

    fn run_non_leaf(&self, subtask_idx: NodeIndex, child_tuple: Tuple) -> Result<Tuple> {
        let subtask = self.tree.node_weight(subtask_idx).expect("must be found");
        match subtask {
            QuerySubtaskNode::Projection(projection_subtask) => projection_subtask.run(child_tuple),
            QuerySubtaskNode::GroupAggregateWindow(subtask) => subtask.run(child_tuple),
            QuerySubtaskNode::Collect(_) => unreachable!(),
        }
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    fn run_leaf(&self, subtask_idx: NodeIndex, context: &TaskContext) -> Option<QuerySubtaskOut> {
        let subtask = self.tree.node_weight(subtask_idx).expect("must be found");
        match subtask {
            QuerySubtaskNode::Collect(collect_subtask) => collect_subtask.run(context),
            _ => unreachable!(),
        }
    }

    fn leaf_node_idx(&self) -> NodeIndex {
        self.tree
            .node_references()
            .find_map(|(idx, _)| {
                self.tree
                    .edges_directed(idx, petgraph::Direction::Outgoing)
                    .next()
                    .is_none()
                    .then(|| idx)
            })
            .expect("asserting only 1 leaf currently. TODO multiple leaves")
    }

    fn parent_node_idx(&self, node_idx: NodeIndex) -> Option<NodeIndex> {
        self.tree
            .edges_directed(node_idx, petgraph::Direction::Incoming)
            .next()
            .map(|parent_edge| parent_edge.source())
    }
}
