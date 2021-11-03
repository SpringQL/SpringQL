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
mod pipeline;
mod reactive_executor;

use crate::error::Result;
use autonomous_executor::{CurrentTimestamp, NaiveRowRepository, RowRepository, Scheduler};

use self::{
    autonomous_executor::AutonomousExecutor, command::alter_pipeline_command::AlterPipelineCommand,
    dependency_injection::DependencyInjection, pipeline::Pipeline,
    reactive_executor::ReactiveExecutor,
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
        let pipeline = self.reactive_executor.alter_pipeline(command)?;
        self.autonomous_executor.notify_pipeline_update(pipeline);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::autonomous_executor::test_support::foreign::source::TestSource;
    use super::*;
    use crate::{
        error::SpringError,
        model::name::{PumpName, StreamName},
        stream_engine::autonomous_executor::{
            data::foreign_row::format::json::JsonObject, test_support::foreign::sink::TestSink,
        },
        test_support::setup::setup_test_logger,
    };

    #[test]
    fn test_stream_engine_source_sink() {
        setup_test_logger();

        let json_oracle = JsonObject::fx_trade_oracle();
        let json_ibm = JsonObject::fx_trade_ibm();
        let json_google = JsonObject::fx_trade_google();

        let source = TestSource::start(vec![
            json_oracle.clone(),
            json_ibm.clone(),
            json_google.clone(),
        ])
        .unwrap();

        let sink = TestSink::start().unwrap();

        let fst_trade_source = StreamName::factory("fst_trade_source");
        let fst_trade_sink = StreamName::factory("fst_trade_sink");
        let pu_trade_source_p1 = PumpName::factory("pu_trade_source_p1");

        let mut engine = StreamEngine::new(2);
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

        assert!(matches!(
            sink.receive().unwrap_err(),
            SpringError::Unavailable { .. }
        ));

        engine
            .alter_pipeline(AlterPipelineCommand::fx_alter_pump_start(
                pu_trade_source_p1,
            ))
            .unwrap();

        assert_eq!(JsonObject::new(sink.receive().unwrap()), json_oracle);
        assert_eq!(JsonObject::new(sink.receive().unwrap()), json_ibm);
        assert_eq!(JsonObject::new(sink.receive().unwrap()), json_google);
        assert!(matches!(
            sink.receive().unwrap_err(),
            SpringError::Unavailable { .. }
        ));
    }
}
