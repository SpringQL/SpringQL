use std::rc::Rc;

use self::{final_row::FinalRow, node_executor_tree::NodeExecutorTree};
use crate::{
    dependency_injection::DependencyInjection, error::Result, model::query_plan::QueryPlan,
};

mod final_row;
mod interm_row;
mod node_executor_tree;
mod row_window;

/// Process input row 1-by-1.
#[derive(Debug)]
pub(super) struct QueryExecutor<DI>
where
    DI: DependencyInjection,
{
    node_executor_tree: NodeExecutorTree<DI>,
}

impl<DI> QueryExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(super) fn register(di: Rc<DI>, query_plan: QueryPlan) -> Self {
        Self {
            node_executor_tree: NodeExecutorTree::compile(di, query_plan),
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run(&mut self) -> Result<FinalRow> {
        self.node_executor_tree.run()
    }
}

#[cfg(test)]
impl<DI> QueryExecutor<DI>
where
    DI: DependencyInjection,
{
    fn run_expect(&mut self, expected: Vec<FinalRow>) {
        use crate::error::SpringError;

        for expected_row in expected {
            assert_eq!(self.run().unwrap(), expected_row);
        }
        assert!(matches!(
            self.run().unwrap_err(),
            SpringError::InputTimeout { .. }
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::{
        dependency_injection::test_di::TestDI,
        model::{
            name::PumpName,
            query_plan::{
                query_plan_node::{QueryPlanNode, QueryPlanNodeLeaf},
                QueryPlan,
            },
        },
        stream_engine::executor::data::row::Row,
    };

    use super::*;

    #[test]
    fn test_query_executor_collect() {
        let di = Rc::new(TestDI::default());

        // CREATE STREAM trade(
        //   "timestamp" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );
        let pump_trade_p1 = PumpName::fx_trade_p1();

        // SELECT timestamp, ticker, amount FROM trade;
        let query_plan_leaf = QueryPlanNodeLeaf::factory_with_pump_in(
            di.clone(),
            pump_trade_p1,
            vec![
                Row::fx_trade_oracle(),
                Row::fx_trade_ibm(),
                Row::fx_trade_google(),
            ],
        );

        let query_plan = QueryPlan::new(Rc::new(QueryPlanNode::Leaf(query_plan_leaf)));
        let mut executor = QueryExecutor::<TestDI>::register(di, query_plan);

        executor.run_expect(vec![
            FinalRow::Preserved(Rc::new(Row::fx_trade_oracle())),
            FinalRow::Preserved(Rc::new(Row::fx_trade_ibm())),
            FinalRow::Preserved(Rc::new(Row::fx_trade_google())),
        ]);
    }
}
