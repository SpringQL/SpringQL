use self::{final_row::FinalRow, interm_row::NewRow};
use crate::{error::Result, model::query_plan::QueryPlan};

mod final_row;
mod interm_row;
mod row_window;
mod window_executor;

#[derive(Debug)]
pub(super) struct QueryExecutor {
    query_plan: QueryPlan,

    /// Some(_) means: Output of the query plan is this NewRow.
    /// None means: Output of the query plan is the input of it.
    latest_new_row: Option<NewRow>,
}

impl QueryExecutor {
    pub(super) fn register(query_plan: QueryPlan) -> Self {
        Self {
            query_plan,
            latest_new_row: None,
        }
    }

    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run(&mut self) -> Result<FinalRow> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::{
        dependency_injection::{test_di::TestDI, DependencyInjection},
        error::SpringError,
        model::{
            name::{PumpName, StreamName},
            query_plan::{
                query_plan_node::{operation::LeafOperation, QueryPlanNode, QueryPlanNodeLeaf},
                QueryPlan,
            },
        },
        stream_engine::{executor::data::row::Row, RowRepository},
    };

    use super::*;

    #[test]
    fn test_query_executor_collect() {
        let di = TestDI::default();
        let row_repo = di.row_repository();

        // CREATE STREAM trade(
        //   "timestamp" TIMESTAMP NOT NULL AS ROWTIME,
        //   "ticker" TEXT NOT NULL,
        //   "amount" INTEGER NOT NULL
        // );
        let stream_trade = StreamName::fx_trade();

        let pump_trade_p1 = PumpName::fx_trade_p1();
        let downstream_pumps = vec![pump_trade_p1];

        let row_oracle = Row::fx_trade_oracle();
        let row_ibm = Row::fx_trade_ibm();
        let row_google = Row::fx_trade_google();

        row_repo.emit_owned(row_oracle, &downstream_pumps).unwrap();
        row_repo.emit_owned(row_ibm, &downstream_pumps).unwrap();
        row_repo.emit_owned(row_google, &downstream_pumps).unwrap();

        // SELECT timestamp, ticker, amount FROM trade;
        let query_plan = QueryPlan::new(Rc::new(QueryPlanNode::Leaf(QueryPlanNodeLeaf {
            op: LeafOperation::Collect {
                stream: stream_trade,
            },
        })));
        let mut executor = QueryExecutor::register(query_plan);

        if let FinalRow::Preserved(got_row) = executor.run().unwrap() {
            assert_eq!(*got_row, Row::fx_trade_oracle());
        } else {
            panic!("Expected FinalRow::Preserved");
        }
        if let FinalRow::Preserved(got_row) = executor.run().unwrap() {
            assert_eq!(*got_row, Row::fx_trade_ibm());
        } else {
            panic!("Expected FinalRow::Preserved");
        }
        if let FinalRow::Preserved(got_row) = executor.run().unwrap() {
            assert_eq!(*got_row, Row::fx_trade_google());
        } else {
            panic!("Expected FinalRow::Preserved");
        }

        assert!(matches!(
            executor.run().unwrap_err(),
            SpringError::InputTimeout { .. }
        ));
    }
}
