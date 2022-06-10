// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("stream_engine.md")]

mod autonomous_executor;
pub mod command;
mod in_memory_queue_repository;
mod sql_executor;
pub mod time;

use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::anyhow;

pub use crate::stream_engine::autonomous_executor::SpringValue;
pub use autonomous_executor::{
    row::{
        value::{NnSqlValue, SqlCompareResult, SqlValue},
        Row,
    },
    task::tuple::Tuple,
};

use crate::{
    api::{error::Result, SpringConfig, SpringError},
    pipeline::{Pipeline, QueueName},
    stream_engine::{
        autonomous_executor::AutonomousExecutor,
        command::alter_pipeline_command::AlterPipelineCommand,
        in_memory_queue_repository::InMemoryQueueRepository, sql_executor::SqlExecutor,
    },
};

#[derive(Clone, Debug)]
pub struct EngineMutex(Arc<Mutex<StreamEngine>>);

impl EngineMutex {
    pub fn new(config: &SpringConfig) -> Self {
        let engine = StreamEngine::new(config);
        Self(Arc::new(Mutex::new(engine)))
    }

    /// # Failure
    ///
    /// - `SpringError::ThreadPoisoned`
    pub fn get(&self) -> Result<MutexGuard<'_, StreamEngine>> {
        self.0
            .lock()
            .map_err(|e| {
                anyhow!(
                    "another thread sharing the same stream-engine got panic: {:?}",
                    e
                )
            })
            .map_err(SpringError::SpringQlCoreIo)
    }
}

/// Stream engine has SQL executor and autonomous executor inside.
///
/// Stream engine has Access Methods.
/// External components (sql-processor) call Access Methods to change stream engine's states and get result from it.
#[derive(Debug)]
pub struct StreamEngine {
    sql_executor: SqlExecutor,
    autonomous_executor: AutonomousExecutor,
}

impl StreamEngine {
    /// Setup sequence is drawn in a diagram: <https://github.com/SpringQL/SpringQL/issues/100#issuecomment-1101732796>
    pub fn new(config: &SpringConfig) -> Self {
        Self {
            sql_executor: SqlExecutor::default(),
            autonomous_executor: AutonomousExecutor::new(config),
        }
    }

    pub fn current_pipeline(&self) -> &Pipeline {
        self.sql_executor.current_pipeline()
    }

    pub fn alter_pipeline(&mut self, command: AlterPipelineCommand) -> Result<()> {
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
    /// - `SpringError::Unavailable` when:
    ///   - queue named `queue_name` does not exist.
    pub fn pop_in_memory_queue_non_blocking(
        &mut self,
        queue_name: QueueName,
    ) -> Result<Option<Row>> {
        let q = InMemoryQueueRepository::instance().get(&queue_name)?;
        let row = q.pop_non_blocking();
        Ok(row)
    }
}
