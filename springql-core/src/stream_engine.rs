//! Stream Engine component.
//!
//! Responsible for pipeline management and execution.
//!
//! Stream engine has 2 executors:
//!
//! 1. Reactive executor
//! 2. Autonomous executor
//!
//! Reactive executor quickly changes some status of a pipeline. It does not deal with stream data (Row, to be precise).
//! Autonomous executor deals with stream data.
//!
//! Both reactive executor and autonomous executor instance run at a main thread, while autonomous executor has workers which run at different worker threads.
//! ```

pub(crate) mod command;

mod autonomous_executor;
mod dependency_injection;
mod reactive_executor;

use crate::{error::Result, pipeline::Pipeline};
use autonomous_executor::{CurrentTimestamp, RowRepository, Scheduler};

use self::{
    autonomous_executor::AutonomousExecutor, command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection, reactive_executor::ReactiveExecutor,
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
    pipeline: Pipeline,

    reactive_executor: ReactiveExecutor,
    autonomous_executor: AutonomousExecutor<DI>,
}

impl<DI> StreamEngineDI<DI>
where
    DI: DependencyInjection,
{
    pub(crate) fn new(n_worker_threads: usize) -> Self {
        Self {
            pipeline: Pipeline::default(),
            reactive_executor: ReactiveExecutor::default(),
            autonomous_executor: AutonomousExecutor::new(n_worker_threads),
        }
    }

    pub(crate) fn alter_pipeline(&mut self, command: AlterPipelineCommand) -> Result<()> {
        log::debug!("[StreamEngine] alter_pipeline({:?})", command);
        let pipeline = self.reactive_executor.alter_pipeline(command)?;
        self.autonomous_executor.notify_pipeline_update(pipeline)
    }
}

#[cfg(test)]
mod tests {
    use test_foreign_service::{sink::TestForeignSink, source::TestForeignSource};
    use test_logger::setup_test_logger;

    use super::*;
    use crate::{
        pipeline::name::{PumpName, StreamName},
        stream_engine::autonomous_executor::row::foreign_row::format::json::JsonObject,
    };

    /// Returns sink output in reached order
    fn t_stream_engine_source_sink(
        source_inputs: Vec<serde_json::Value>,
        n_worker_threads: usize,
    ) -> Vec<serde_json::Value> {
        setup_test_logger();

        let source_inputs_len = source_inputs.len();

        let source = TestForeignSource::start(source_inputs).unwrap();
        let sink = TestForeignSink::start().unwrap();

        let fst_trade_source = StreamName::factory("fst_trade_source");
        let fst_trade_sink = StreamName::factory("fst_trade_sink");
        let pu_trade_source_p1 = PumpName::factory("pu_trade_source_p1");

        let mut engine = StreamEngine::new(n_worker_threads);
        engine
            .alter_pipeline(
                AlterPipelineCommand::fx_create_foreign_stream_trade_with_source_server(
                    fst_trade_source.clone(),
                    source.host_ip(),
                    source.port(),
                ),
            )
            .unwrap();
        engine
            .alter_pipeline(
                AlterPipelineCommand::fx_create_foreign_stream_trade_with_sink_server(
                    fst_trade_sink.clone(),
                    sink.host_ip(),
                    sink.port(),
                ),
            )
            .unwrap();
        engine
            .alter_pipeline(AlterPipelineCommand::fx_create_pump(
                pu_trade_source_p1.clone(),
                fst_trade_source,
                fst_trade_sink,
            ))
            .unwrap();

        assert!(sink.receive().is_err());

        engine
            .alter_pipeline(AlterPipelineCommand::fx_alter_pump_start(
                pu_trade_source_p1,
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

        let received = t_stream_engine_source_sink(
            vec![json_oracle.clone(), json_ibm.clone(), json_google.clone()],
            1,
        );

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

        let received = t_stream_engine_source_sink(
            vec![json_oracle.clone(), json_ibm.clone(), json_google.clone()],
            2,
        );

        // a worker might be faster than the other.
        assert!([json_oracle.clone(), json_ibm.clone(), json_google.clone()]
            .contains(received.get(0).unwrap()));
        assert!([json_oracle.clone(), json_ibm.clone(), json_google.clone()]
            .contains(received.get(1).unwrap()));
        assert!([json_oracle, json_ibm, json_google].contains(received.get(2).unwrap()));
    }
}
