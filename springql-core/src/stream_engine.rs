// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Stream Engine component.
//!
//! Responsible for pipeline management and execution.
//!
//! Stream engine has 2 executors:
//!
//! 1. SQL executor
//! 2. Autonomous executor
//!
//! SQL executor receives commands from user interface to quickly change some status of a pipeline.
//! It does not deal with stream data (Row, to be precise).
//!
//! Autonomous executor deals with stream data.
//!
//! Both SQL executor and autonomous executor instance run at a main thread, while autonomous executor has workers which run at different worker threads.
//!
//! # Entities inside Stream Engine
//!
//! ![Entities inside Stream Engine](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-entity.svg)
//!
//! # Communication between entities
//!
//! Workers in AutonomousExecutor interact via EventQueue (Choreography-based Saga pattern).
//!
//! ![Communication between entities](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-communication.svg)

pub(crate) mod command;
pub(crate) mod time;

mod autonomous_executor;
mod in_memory_queue_repository;
mod sql_executor;

pub(crate) use autonomous_executor::{
    row::value::{
        sql_convertible::SpringValue,
        sql_value::{nn_sql_value::NnSqlValue, sql_compare_result::SqlCompareResult, SqlValue},
    },
    task::tuple::Tuple,
    SinkRow,
};

use crate::{
    error::Result,
    low_level_rs::SpringConfig,
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

    /// Blocking call
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue_name` does not exist.
    pub(crate) fn pop_in_memory_queue(&mut self, queue_name: QueueName) -> Result<SinkRow> {
        let q = InMemoryQueueRepository::instance().get(&queue_name)?;
        let row = q.pop();
        Ok(row)
    }
}
