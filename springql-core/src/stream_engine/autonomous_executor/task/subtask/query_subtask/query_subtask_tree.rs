mod query_subtask_node;

use std::sync::Arc;

use self::query_subtask_node::QuerySubtaskNode;

use super::final_row::SubtaskRow;
use super::interm_row::NewRow;
use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::command::query_plan::QueryPlan;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug)]
pub(super) struct QuerySubtaskTree {
    root: QuerySubtaskNode, // TODO use petgraph

    /// Some(_) means: Output of the query plan is this NewRow.
    /// None means: Output of the query plan is the input of it.
    latest_new_row: Option<NewRow>,
}

impl From<&QueryPlan> for QuerySubtaskTree {
    fn from(query_plan: &QueryPlan) -> Self {
        todo!()
    }
}

impl QuerySubtaskTree {
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(super) fn run<DI: DependencyInjection>(
        &mut self,
        context: &TaskContext<DI>,
    ) -> Result<SubtaskRow> {
        let row = Self::run_dfs_post_order::<DI>(&self.root, &mut self.latest_new_row, context)?;

        if let Some(new_row) = self.latest_new_row.take() {
            Ok(SubtaskRow::NewlyCreated(new_row.into()))
        } else {
            Ok(SubtaskRow::Preserved(row))
        }
    }

    fn run_dfs_post_order<DI: DependencyInjection>(
        subtask: &QuerySubtaskNode,
        latest_new_row: &mut Option<NewRow>,
        context: &TaskContext<DI>,
    ) -> Result<Arc<Row>> {
        match subtask {
            QuerySubtaskNode::Collect(collect_subtask) => collect_subtask.run::<DI>(context),
            QuerySubtaskNode::Stream(_) => todo!(),
            QuerySubtaskNode::Window(_) => todo!(),
        }
    }
}
