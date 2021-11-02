pub(in crate::stream_engine) mod task;

pub(super) mod data;

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
pub(super) mod test_support;

/// Executor of pipeline's stream data.
///
/// All interface methods are called from main thread, while `new()` incur spawn worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    /// Writer: Main thread. Write on pipeline update.
    scheduler_write: SchedulerWrite<DI>,
    /// Reader: Worker threads. Read on task request.
    scheduler_read: SchedulerRead<DI>,

    worker_pool: WorkerPool,

    row_repo: Arc<DI::RowRepositoryType>,
}

impl<DI> AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let scheduler = Arc::new(RwLock::new(DI::SchedulerType::default()));
        let scheduler_write = SchedulerWrite::new(scheduler.clone());
        let scheduler_read = SchedulerRead::new(scheduler.clone());

        let row_repo = Arc::new(DI::RowRepositoryType::default());

        Self {
            scheduler_write,
            scheduler_read: scheduler_read.clone(),
            worker_pool: WorkerPool::new::<DI>(n_worker_threads, scheduler_read, row_repo.clone()),
            row_repo,
        }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(&self, pipeline: Pipeline) {
        self.scheduler_write
            .write_lock()
            .notify_pipeline_update(pipeline);

        self.row_repo
            .reset(self.scheduler_read.read_lock().task_graph().all_tasks())
    }
}
