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
mod row_window;

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
            QuerySubtaskNode::SlidingWindow(_) => todo!(),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use test_logger::setup_test_logger;

    use crate::{
        pipeline::name::{ColumnName, PumpName, StreamName},
        stream_engine::autonomous_executor::{row::Row, task::task_id::TaskId},
        stream_engine::{
            autonomous_executor::NaiveRowRepository, dependency_injection::test_di::TestDI,
        },
        stream_engine::{autonomous_executor::RowRepository, command::query_plan::QueryPlan},
    };

    use super::*;

    #[test]
    fn test_query_subtask_collect() {
        setup_test_logger();

        let source_task = TaskId::from_source_server(StreamName::fx_trade_source());
        let pump_trade_p1 = PumpName::fx_trade_p1();
        let downstream_tasks = vec![TaskId::from_pump(pump_trade_p1)];
        let context = TaskContext::factory_with_1_level_downstreams(source_task, downstream_tasks);

        let input = vec![
            Row::fx_trade_oracle(),
            Row::fx_trade_ibm(),
            Row::fx_trade_google(),
        ];
        for row in input {
            let row_repo: Arc<NaiveRowRepository> = context.row_repository();
            row_repo.emit(row, &[context.task()]).unwrap();
        }

        // CREATE STREAM trade(
        //   "ts" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );

        // SELECT ts, ticker, amount FROM trade;

        let query_plan = QueryPlan::fx_collect_projection(
            StreamName::fx_trade_source(),
            vec![
                ColumnName::fx_timestamp(),
                ColumnName::fx_ticker(),
                ColumnName::fx_amount(),
            ],
        );
        let mut subtask = QuerySubtask::from(&query_plan);

        subtask.run_expect::<TestDI>(
            vec![
                Row::fx_trade_oracle(),
                Row::fx_trade_ibm(),
                Row::fx_trade_google(),
            ],
            &context,
        );
    }
}
