use std::rc::Rc;

use self::{final_row::FinalRow, node_executor_tree::NodeExecutorTree};
use crate::{
    error::Result, model::query_plan::QueryPlan,
    stream_engine::dependency_injection::DependencyInjection,
};

mod final_row;
mod interm_row;
mod node_executor_tree;
mod row_window;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(super) struct QueryExecutor {
    node_executor_tree: NodeExecutorTree,
}

impl QueryExecutor {
    pub(super) fn register(query_plan: QueryPlan) -> Self {
        Self {
            node_executor_tree: NodeExecutorTree::compile(query_plan),
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run<DI: DependencyInjection>(
        &mut self,
        row_repo: &DI::RowRepositoryType,
    ) -> Result<FinalRow> {
        self.node_executor_tree.run::<DI>(row_repo)
    }
}

#[cfg(test)]
impl QueryExecutor {
    fn run_expect<DI: DependencyInjection>(
        &mut self,
        expected: Vec<FinalRow>,
        row_repo: &DI::RowRepositoryType,
    ) {
        use crate::error::SpringError;

        for expected_row in expected {
            assert_eq!(self.run::<DI>(row_repo).unwrap(), expected_row);
        }
        assert!(matches!(
            self.run::<DI>(row_repo).unwrap_err(),
            SpringError::InputTimeout { .. }
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::{
        model::{
            name::PumpName,
            query_plan::{
                query_plan_node::{QueryPlanNode, QueryPlanNodeLeaf},
                QueryPlan,
            },
        },
        stream_engine::autonomous_executor::data::row::{self, Row},
        stream_engine::dependency_injection::test_di::TestDI,
    };

    use super::*;

    #[test]
    fn test_query_executor_collect() {
        let row_repo = <TestDI as DependencyInjection>::RowRepositoryType::default();

        // CREATE STREAM trade(
        //   "timestamp" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );
        let pump_trade_p1 = PumpName::fx_trade_p1();

        // SELECT timestamp, ticker, amount FROM trade;
        let query_plan_leaf = QueryPlanNodeLeaf::factory_with_pump_in::<TestDI>(
            pump_trade_p1,
            vec![
                Row::fx_trade_oracle(),
                Row::fx_trade_ibm(),
                Row::fx_trade_google(),
            ],
            &row_repo,
        );

        let query_plan = QueryPlan::new(Rc::new(QueryPlanNode::Leaf(query_plan_leaf)));
        let mut executor = QueryExecutor::register(query_plan);

        executor.run_expect::<TestDI>(
            vec![
                FinalRow::Preserved(Rc::new(Row::fx_trade_oracle())),
                FinalRow::Preserved(Rc::new(Row::fx_trade_ibm())),
                FinalRow::Preserved(Rc::new(Row::fx_trade_google())),
            ],
            &row_repo,
        );
    }
}
