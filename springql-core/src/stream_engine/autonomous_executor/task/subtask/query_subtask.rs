use petgraph::graph::DiGraph;

use self::{final_row::SubtaskRow, query_subtask_node::QuerySubtaskNode};
use crate::{
    error::Result,
    stream_engine::command::query_plan::{child_direction::ChildDirection, QueryPlan},
    stream_engine::{
        autonomous_executor::task::task_context::TaskContext,
        dependency_injection::DependencyInjection,
    },
};

mod final_row;
mod interm_row;
mod query_subtask_node;
mod row_window;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(super) struct QuerySubtask {
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
    pub(super) fn run<DI: DependencyInjection>(
        &mut self,
        context: &TaskContext<DI>,
    ) -> Result<SubtaskRow> {
        Self::run_dfs_post_order::<DI>(self.root(), context)
    }

    fn run_dfs_post_order<DI: DependencyInjection>(
        subtask: &QuerySubtaskNode,
        context: &TaskContext<DI>,
    ) -> Result<SubtaskRow> {
        match subtask {
            QuerySubtaskNode::Collect(collect_subtask) => {
                let row = collect_subtask.run::<DI>(context)?;
                Ok(SubtaskRow::Preserved(row))
            }
            QuerySubtaskNode::Stream(_) => todo!(),
            QuerySubtaskNode::Window(_) => todo!(),
        }
    }

    fn root(&self) -> &QuerySubtaskNode {
        self.tree
            .node_indices()
            .find_map(|i| {
                (self
                    .tree
                    .edges_directed(i, petgraph::Direction::Incoming)
                    .count()
                    == 0)
                    .then(|| self.tree.node_weight(i).unwrap())
            })
            .unwrap()
    }
}

#[cfg(test)]
impl QuerySubtask {
    fn run_expect<DI: DependencyInjection>(
        &mut self,
        expected: Vec<SubtaskRow>,
        context: &TaskContext<DI>,
    ) {
        use crate::error::SpringError;

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
        pipeline::name::{PumpName, StreamName},
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

        let task = TaskId::from_source_server(StreamName::fx_trade_source());
        let pump_trade_p1 = PumpName::fx_trade_p1();
        let downstream_tasks = vec![TaskId::from_pump(pump_trade_p1)];
        let context = TaskContext::factory_with_1_level_downstreams(task, downstream_tasks);

        let input = vec![
            Row::fx_trade_oracle(),
            Row::fx_trade_ibm(),
            Row::fx_trade_google(),
        ];
        for row in input {
            let row_repo: Arc<NaiveRowRepository> = context.row_repository();
            row_repo.emit_owned(row, &[context.task()]).unwrap();
        }

        // CREATE STREAM trade(
        //   "ts" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );

        // SELECT ts, ticker, amount FROM trade;

        let query_plan = QueryPlan::fx_collect(StreamName::fx_trade_source());
        let mut subtask = QuerySubtask::from(&query_plan);

        subtask.run_expect::<TestDI>(
            vec![
                SubtaskRow::Preserved(Arc::new(Row::fx_trade_oracle())),
                SubtaskRow::Preserved(Arc::new(Row::fx_trade_ibm())),
                SubtaskRow::Preserved(Arc::new(Row::fx_trade_google())),
            ],
            &context,
        );
    }
}
