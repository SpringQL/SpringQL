pub(in crate::stream_engine) mod task;

pub(self) mod data;
pub(self) mod exec;
pub(self) mod server;

mod scheduler;
mod worker_pool;

use crate::error::Result;
use std::sync::{Arc, RwLock};

pub(in crate::stream_engine) use data::{
    CurrentTimestamp, NaiveRowRepository, RowRepository, Timestamp,
};
pub(in crate::stream_engine) use scheduler::{FlowEfficientScheduler, Scheduler};

use self::{
    scheduler::{scheduler_read::SchedulerRead, scheduler_write::SchedulerWrite},
    worker_pool::WorkerPool,
};

use super::{dependency_injection::DependencyInjection, pipeline::Pipeline};

#[cfg(test)]
pub mod test_support;

/// Executor of pipeline's stream data.
///
/// All interface methods are called from main thread, while `new()` incur spawn worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    /// Writer: Main thread. Write on pipeline update.
    /// Reader: Worker threads. Read on task request.
    scheduler_write: SchedulerWrite<DI>,

    worker_pool: WorkerPool,
}

impl<DI> AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let scheduler = Arc::new(RwLock::new(DI::SchedulerType::default()));
        let scheduler_write = SchedulerWrite::new(scheduler.clone());
        let scheduler_read = SchedulerRead::new(scheduler.clone());

        Self {
            scheduler_write,
            worker_pool: WorkerPool::new::<DI>(n_worker_threads, scheduler_read),
        }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(&self, pipeline: Pipeline) {
        self.scheduler_write.write_lock().notify_pipeline_update(pipeline)
    }
}
