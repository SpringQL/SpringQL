// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::command::insert_plan::InsertPlan;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtask {}

impl From<&InsertPlan> for InsertSubtask {
    fn from(_plan: &InsertPlan) -> Self {
        Self {
            // TODO
            // stream: plan.stream().clone(),
            // insert_columns: plan.insert_columns().clone(),
        }
    }
}

impl InsertSubtask {
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - Input from a source stream is not available within timeout period.
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        row: Row,
        context: &TaskContext,
    ) -> Result<()> {
        let row_repo = context.row_repository();

        // TODO modify row if necessary
        row_repo.emit(row, &context.downstream_tasks())
    }
}
