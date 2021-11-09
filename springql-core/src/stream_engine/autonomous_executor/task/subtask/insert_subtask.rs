use crate::error::Result;
use crate::pipeline::name::{ColumnName, StreamName};
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::command::insert_plan::InsertPlan;
use crate::stream_engine::{
    autonomous_executor::task::task_context::TaskContext, dependency_injection::DependencyInjection,
};

use super::subtask_row::SubtaskRow;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtask {
    stream: StreamName,
    insert_columns: Vec<ColumnName>,
}

impl From<&InsertPlan> for InsertSubtask {
    fn from(plan: &InsertPlan) -> Self {
        Self {
            stream: plan.stream().clone(),
            insert_columns: plan.insert_columns().clone(),
        }
    }
}

impl InsertSubtask {
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        row: SubtaskRow,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        let row_repo = context.row_repository();

        // TODO modify row if necessary

        match row {
            SubtaskRow::Preserved(r) => row_repo.emit(r, &context.downstream_tasks()),
            SubtaskRow::NewlyCreated(r) => row_repo.emit_owned(r, &context.downstream_tasks()),
        }
    }
}
