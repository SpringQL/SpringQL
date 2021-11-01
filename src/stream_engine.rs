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

pub(crate) mod pipeline;

mod autonomous_executor;
mod dependency_injection;
mod reactive_executor;

use crate::error::Result;
use autonomous_executor::{CurrentTimestamp, NaiveRowRepository, RowRepository, Scheduler};

use self::{
    autonomous_executor::AutonomousExecutor, dependency_injection::DependencyInjection,
    pipeline::Pipeline, reactive_executor::ReactiveExecutor,
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

    // pub(crate) fn alter_pipeline(&mut self, command: AlterPipelineCommand) -> Result<()> {
    //     let pipeline = self.reactive_executor.alter_pipeline(commad)?;
    //     self.autonomous_executor.update_pipeline(pipeline);
    //     Ok(())
    // }
}
