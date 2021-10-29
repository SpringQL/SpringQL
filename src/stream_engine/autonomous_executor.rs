pub(self) mod data;
pub(self) mod exec;
pub(self) mod server;

mod worker_pool;

use std::sync::{Arc, Mutex};

pub(crate) use data::{CurrentTimestamp, RowRepository, Timestamp};

#[cfg(test)]
pub(crate) use data::TestRowRepository;

use self::worker_pool::WorkerPool;

#[cfg(test)]
pub mod test_support;

/// Executor of pipeline's stream data.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor {
    /// On empty: Asks scheduler to give a runnable task.
    worker_pool: WorkerPool,
}

impl AutonomousExecutor {
    pub(crate) fn new(n_worker_threads: usize, pipeline: PipelineRead) -> Self {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(pipeline)));

        Self {
            worker_pool: WorkerPool::new(n_worker_threads, scheduler),
        }
    }
}
