// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Stream Engine component.
//!
//! Responsible for pipeline management and execution.
//!
//! Stream engine has 2 executors:
//!
//! 1. SQL executor
//! 2. Autonomous executor
//!
//! Reactive executor receives commands from user interface to quickly change some status of a pipeline.
//! It also gets interruption from `PerformanceMonitorWorker` to change task execution policy.
//! It does not deal with stream data (Row, to be precise).
//!
//! Autonomous executor deals with stream data.
//!
//! Both reactive executor and autonomous executor instance run at a main thread, while autonomous executor has workers which run at different worker threads.
//!
//! # `*Executor` structure
//!
//! - StreamEngine
//!   - SqlExecutor (main thread; alters pipeline)
//!   - AutonomousExecutor
//!     - MemoryStateMachine (1 thread)
//!     - TaskExecutor
//!       - SourceWorkerPool (N threads; runs source tasks with SourceScheduler)
//!       - GenericWorkerPool (M threads; runs pump and sink tasks with a dynamically-chosen scheduler)
//!     - PerformanceMonitorWorker (1 thread)
//!
//! # `*Executor` interaction
//!
//! ## SQL command flow
//!
//! 1. SQL from user (via C API, for example) -> [api](crate::api) -> SqlExecutor.
//! 2. SqlExecutor locks TaskExecutor, kicks Purger, and notifies pipeline update to TaskExecutor.
//!
//! ## Memory state transition
//!
//! 1. PerformanceMonitorWorker periodically reports performance metrics to MemoryStateMachine.
//! 2. When MemoryStateMachine detect condition to transit memory state:
//!   - [* -> Moderate] MemoryStateMachine notifies state transition to TaskExecutor and TaskExecutor sets task scheduler to FlowEfficientScheduler.
//!   - [* -> Severe] MemoryStateMachine notifies state transition to TaskExecutor and TaskExecutor sets task scheduler to MemoryReducingScheduler.
//!   - [* -> Critical] MemoryStateMachine notifies state transition to TaskExecutor; and TaskExecutor locks source workers and generic workers, kicks Purger, and waits for next performance metrics.

pub(crate) mod command;

mod autonomous_executor;
mod dependency_injection;
mod in_memory_queue_repository;
mod sql_executor;

pub(crate) use autonomous_executor::{
    row::value::{sql_convertible::SqlConvertible, sql_value::SqlValue},
    SinkRow,
};

use crate::{error::Result, pipeline::name::QueueName};
use autonomous_executor::{CurrentTimestamp, RowRepository};

use self::{
    autonomous_executor::AutonomousExecutor, command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection, in_memory_queue_repository::InMemoryQueueRepository,
    sql_executor::SqlExecutor,
};

#[cfg(not(test))]
pub(crate) type StreamEngine = StreamEngineDI<dependency_injection::prod_di::ProdDI>;
#[cfg(test)]
pub(crate) type StreamEngine = StreamEngineDI<dependency_injection::test_di::TestDI>;

/// Stream engine has reactive executor and autonomous executor inside.
///
/// Stream engine has Access Methods.
/// External components (sql-processor) call Access Methods to change stream engine's states and get result from it.
#[derive(Debug)]
pub(crate) struct StreamEngineDI<DI: DependencyInjection> {
    reactive_executor: SqlExecutor,
    autonomous_executor: AutonomousExecutor<DI>,
}

impl<DI> StreamEngineDI<DI>
where
    DI: DependencyInjection,
{
    pub(crate) fn new(n_worker_threads: usize) -> Self {
        Self {
            reactive_executor: SqlExecutor::default(),
            autonomous_executor: AutonomousExecutor::new(n_worker_threads),
        }
    }

    pub(crate) fn alter_pipeline(&mut self, command: AlterPipelineCommand) -> Result<()> {
        log::debug!("[StreamEngine] alter_pipeline({:?})", command);
        let pipeline = self.reactive_executor.alter_pipeline(command)?;
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

#[cfg(test)]
mod tests {
    use springql_foreign_service::{
        sink::ForeignSink,
        source::{source_input::ForeignSourceInput, ForeignSource},
    };
    use springql_test_logger::setup_test_logger;

    use super::*;
    use crate::{
        pipeline::name::{PumpName, StreamName},
        stream_engine::autonomous_executor::row::foreign_row::format::json::JsonObject,
    };

    /// Returns sink output in reached order
    fn t_stream_engine_source_sink(
        source_input: Vec<serde_json::Value>,
        n_worker_threads: usize,
    ) -> Vec<serde_json::Value> {
        setup_test_logger();

        let source_inputs_len = source_input.len();

        let source =
            ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_input)).unwrap();
        let sink = ForeignSink::start().unwrap();

        let fst_trade_source = StreamName::factory("fst_trade_source");
        let fst_trade_sink = StreamName::factory("fst_trade_sink");
        let pu_trade_source_p1 = PumpName::factory("pu_trade_source_p1");

        let mut engine = StreamEngine::new(n_worker_threads);
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_source_stream_trade(
                fst_trade_source.clone(),
            ))
            .unwrap();
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_sink_stream_trade(
                fst_trade_sink.clone(),
            ))
            .unwrap();
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_pump(
                pu_trade_source_p1,
                fst_trade_source.clone(),
                fst_trade_sink.clone(),
            ))
            .unwrap();
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_sink_writer_trade(
                fst_trade_sink,
                sink.host_ip(),
                sink.port(),
            ))
            .unwrap();
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_source_reader_trade(
                fst_trade_source,
                source.host_ip(),
                source.port(),
            ))
            .unwrap();

        let mut received = Vec::new();
        for _ in 0..source_inputs_len {
            received.push(sink.receive().unwrap());
        }
        assert!(sink.receive().is_err());

        received
    }

    #[test]
    fn test_stream_engine_source_sink_single_thread() {
        setup_test_logger();

        let json_oracle: serde_json::Value = JsonObject::fx_trade_oracle().into();
        let json_ibm: serde_json::Value = JsonObject::fx_trade_ibm().into();
        let json_google: serde_json::Value = JsonObject::fx_trade_google().into();

        let input = vec![json_oracle.clone(), json_ibm.clone(), json_google.clone()];
        let received = t_stream_engine_source_sink(input, 1);

        assert_eq!(received.get(0).unwrap(), &json_oracle);
        assert_eq!(received.get(1).unwrap(), &json_ibm);
        assert_eq!(received.get(2).unwrap(), &json_google);
    }

    #[test]
    fn test_stream_engine_source_sink_multi_thread() {
        setup_test_logger();

        let json_oracle: serde_json::Value = JsonObject::fx_trade_oracle().into();
        let json_ibm: serde_json::Value = JsonObject::fx_trade_ibm().into();
        let json_google: serde_json::Value = JsonObject::fx_trade_google().into();

        let input = vec![json_oracle.clone(), json_ibm.clone(), json_google.clone()];
        let received = t_stream_engine_source_sink(input, 1);

        // a worker might be faster than the other.
        assert!([json_oracle.clone(), json_ibm.clone(), json_google.clone()]
            .contains(received.get(0).unwrap()));
        assert!([json_oracle.clone(), json_ibm.clone(), json_google.clone()]
            .contains(received.get(1).unwrap()));
        assert!([json_oracle, json_ibm, json_google].contains(received.get(2).unwrap()));
    }
}
