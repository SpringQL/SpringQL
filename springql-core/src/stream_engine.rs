// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("stream_engine.md")]

pub use autonomous_executor::SpringValue;

pub(crate) mod command;
pub(crate) mod time;

mod autonomous_executor;
mod in_memory_queue_repository;
mod sql_executor;

pub(crate) use autonomous_executor::{
    row::value::sql_value::{
        nn_sql_value::NnSqlValue, sql_compare_result::SqlCompareResult, SqlValue,
    },
    task::tuple::Tuple,
    SinkRow,
};

use crate::{
    api::error::Result,
    api::low_level_rs::SpringConfig,
    pipeline::{name::QueueName, Pipeline},
};

use self::{
    autonomous_executor::AutonomousExecutor, command::alter_pipeline_command::AlterPipelineCommand,
    in_memory_queue_repository::InMemoryQueueRepository, sql_executor::SqlExecutor,
};

/// Stream engine has SQL executor and autonomous executor inside.
///
/// Stream engine has Access Methods.
/// External components (sql-processor) call Access Methods to change stream engine's states and get result from it.
#[derive(Debug)]
pub(crate) struct StreamEngine {
    sql_executor: SqlExecutor,
    autonomous_executor: AutonomousExecutor,
}

impl StreamEngine {
    /// Setup sequence is drawn in a diagram: <https://github.com/SpringQL/SpringQL/issues/100#issuecomment-1101732796>
    pub(crate) fn new(config: &SpringConfig) -> Self {
        Self {
            sql_executor: SqlExecutor::default(),
            autonomous_executor: AutonomousExecutor::new(config),
        }
    }

    pub(crate) fn current_pipeline(&self) -> &Pipeline {
        self.sql_executor.current_pipeline()
    }

    pub(crate) fn alter_pipeline(&mut self, command: AlterPipelineCommand) -> Result<()> {
        log::debug!("[StreamEngine] alter_pipeline({:?})", command);
        let pipeline = self.sql_executor.alter_pipeline(command)?;
        self.autonomous_executor.notify_pipeline_update(pipeline)
    }

    /// # Returns
    ///
    /// - `Ok(Some)` when at least a row is in the queue.
    /// - `None` when no row is in the queue.
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue_name` does not exist.
    pub(crate) fn pop_in_memory_queue_non_blocking(
        &mut self,
        queue_name: QueueName,
    ) -> Result<Option<SinkRow>> {
        let q = InMemoryQueueRepository::instance().get(&queue_name)?;
        let row = q.pop_non_blocking();
        Ok(row)
    }
}
