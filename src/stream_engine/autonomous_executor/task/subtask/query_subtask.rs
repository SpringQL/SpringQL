use self::{final_row::FinalRow, query_subtask_tree::QuerySubtaskTree};
use crate::{
    error::Result,
    model::query_plan::QueryPlan,
    stream_engine::{
        autonomous_executor::task::task_context::TaskContext,
        dependency_injection::DependencyInjection,
    },
};

mod final_row;
mod interm_row;
mod query_subtask_tree;
mod row_window;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(super) struct QuerySubtask {
    query_subtask_tree: QuerySubtaskTree,
}

impl QuerySubtask {
    pub(super) fn register(query_plan: QueryPlan) -> Self {
        Self {
            query_subtask_tree: QuerySubtaskTree::compile(query_plan),
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run<DI: DependencyInjection>(
        &mut self,
        context: &TaskContext<DI>,
    ) -> Result<FinalRow> {
        self.query_subtask_tree.run::<DI>(context)
    }
}

#[cfg(test)]
impl QuerySubtask {
    fn run_expect<DI: DependencyInjection>(
        &mut self,
        expected: Vec<FinalRow>,
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
    use std::{rc::Rc, sync::Arc};

    use crate::{
        model::query_plan::{
            query_plan_node::{QueryPlanNode, QueryPlanNodeLeaf},
            QueryPlan,
        },
        pipeline::name::{PumpName, StreamName},
        stream_engine::autonomous_executor::{row::Row, task::task_id::TaskId},
        stream_engine::dependency_injection::test_di::TestDI,
        test_support::setup::setup_test_logger,
    };

    use super::*;

    #[test]
    fn test_query_subtask_collect() {
        setup_test_logger();

        let task = TaskId::from_source_server(StreamName::fx_trade_source());
        let pump_trade_p1 = PumpName::fx_trade_p1();
        let downstream_tasks = vec![TaskId::from_pump(pump_trade_p1)];
        let context = TaskContext::factory_with_1_level_downstreams(task, downstream_tasks);

        // CREATE STREAM trade(
        //   "timestamp" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );

        // SELECT timestamp, ticker, amount FROM trade;
        let query_plan_leaf = QueryPlanNodeLeaf::factory_with_task_in::<TestDI>(
            vec![
                Row::fx_trade_oracle(),
                Row::fx_trade_ibm(),
                Row::fx_trade_google(),
            ],
            &context,
        );

        let query_plan = QueryPlan::new(Rc::new(QueryPlanNode::Leaf(query_plan_leaf)));
        let mut executor = QuerySubtask::register(query_plan);

        executor.run_expect::<TestDI>(
            vec![
                FinalRow::Preserved(Arc::new(Row::fx_trade_oracle())),
                FinalRow::Preserved(Arc::new(Row::fx_trade_ibm())),
                FinalRow::Preserved(Arc::new(Row::fx_trade_google())),
            ],
            &context,
        );
    }
}
