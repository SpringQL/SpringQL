// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::{EdgeRef, IntoNodeReferences},
};

use self::query_subtask_node::QuerySubtaskNode;
use crate::{
    error::Result,
    stream_engine::{
        autonomous_executor::row::Row,
        command::query_plan::{child_direction::ChildDirection, QueryPlan},
    },
    stream_engine::{
        autonomous_executor::task::task_context::TaskContext,
        dependency_injection::DependencyInjection,
    },
};

mod interm_row;
mod query_subtask_node;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtask {
    tree: DiGraph<QuerySubtaskNode, ChildDirection>,
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
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<Row> {
        let mut next_idx = self.leaf_node_idx();
        let mut next_row = self.run_leaf::<DI>(next_idx, context)?;

        while let Some(parent_idx) = self.parent_node_idx(next_idx) {
            next_idx = parent_idx;
            next_row = self.run_non_leaf::<DI>(next_idx, next_row)?;
        }

        Ok(next_row)
    }

    fn run_non_leaf<DI>(&self, subtask_idx: NodeIndex, downstream_row: Row) -> Result<Row>
    where
        DI: DependencyInjection,
    {
        let subtask = self.tree.node_weight(subtask_idx).expect("must be found");
        match subtask {
            QuerySubtaskNode::Projection(projection_subtask) => {
                projection_subtask.run::<DI>(downstream_row)
            }
            QuerySubtaskNode::Collect(_) => unreachable!(),
        }
    }

    fn run_leaf<DI: DependencyInjection>(
        &self,
        subtask_idx: NodeIndex,
        context: &TaskContext<DI>,
    ) -> Result<Row> {
        let subtask = self.tree.node_weight(subtask_idx).expect("must be found");
        match subtask {
            QuerySubtaskNode::Collect(collect_subtask) => collect_subtask.run::<DI>(context),
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

#[cfg(test)]
impl QuerySubtask {
    fn run_expect<DI: DependencyInjection>(
        &mut self,
        expected: Vec<Row>,
        context: &TaskContext<DI>,
    ) {
        use crate::error::SpringError;
        use pretty_assertions::assert_eq;

        for expected_row in expected {
            assert_eq!(self.run::<DI>(context).unwrap(), expected_row);
        }
        assert!(matches!(
            self.run::<DI>(context).unwrap_err(),
            SpringError::InputTimeout { .. }
        ));
    }
}
