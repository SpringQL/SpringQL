pub(self) mod data;
pub(self) mod exec;
pub(self) mod server;

mod pipeline_read;
mod scheduler;
mod task;
mod worker_pool;

use std::sync::{Arc, Mutex};

pub(in crate::stream_engine) use data::{CurrentTimestamp, RowRepository, Timestamp};
pub(in crate::stream_engine) use scheduler::{FlowEfficientScheduler, Scheduler};

#[cfg(test)]
pub(crate) use data::TestRowRepository;

use self::{pipeline_read::PipelineRead, worker_pool::WorkerPool};

use super::dependency_injection::DependencyInjection;

#[cfg(test)]
pub mod test_support;

/// Executor of pipeline's stream data.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor {
    /// On empty: Asks scheduler to give a runnable task.
    worker_pool: WorkerPool,
}

impl AutonomousExecutor {
    pub(crate) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        pipeline: PipelineRead,
    ) -> Self {
        let scheduler = Arc::new(Mutex::new(DI::SchedulerType::new(pipeline)));

        Self {
            worker_pool: WorkerPool::new::<DI>(n_worker_threads, scheduler),
        }
    }
}
