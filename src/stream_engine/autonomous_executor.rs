pub(in crate::stream_engine) mod server_instance;
pub(in crate::stream_engine) mod task;

pub(self) mod exec;
pub(super) mod row;

mod scheduler;
mod worker_pool;

use crate::error::Result;
use std::sync::{Arc, RwLock};

pub(in crate::stream_engine) use row::{
    CurrentTimestamp, NaiveRowRepository, RowRepository, Timestamp,
};
pub(in crate::stream_engine) use scheduler::{FlowEfficientScheduler, Scheduler};

use self::{
    scheduler::{scheduler_read::SchedulerRead, scheduler_write::SchedulerWrite},
    server_instance::server_repository::ServerRepository,
    worker_pool::WorkerPool,
};

use super::{dependency_injection::DependencyInjection, pipeline::Pipeline};

#[cfg(test)]
pub(super) mod test_support;

/// Executor of pipeline's stream data.
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
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
    server_repo: Arc<ServerRepository>,
}

impl<DI> AutonomousExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine) fn new(n_worker_threads: usize) -> Self {
        let scheduler = Arc::new(RwLock::new(DI::SchedulerType::default()));
        let scheduler_write = SchedulerWrite::new(scheduler.clone());
        let scheduler_read = SchedulerRead::new(scheduler);

        let row_repo = Arc::new(DI::RowRepositoryType::default());
        let server_repo = Arc::new(ServerRepository::default());

        Self {
            scheduler_write,
            scheduler_read: scheduler_read.clone(),
            worker_pool: WorkerPool::new::<DI>(
                n_worker_threads,
                scheduler_read,
                row_repo.clone(),
                server_repo.clone(),
            ),
            row_repo,
            server_repo,
        }
    }

    pub(in crate::stream_engine) fn notify_pipeline_update(
        &self,
        pipeline: Pipeline,
    ) -> Result<()> {
        let mut scheduler = self.scheduler_write.write_lock();
        // 1. Worker executing main_loop (having read lock to scheduler) continues its task.
        // 2. Enter write lock.
        // 3. (Worker cannot get read lock to schedule to start next task)

        self.row_repo.reset(scheduler.task_graph().all_tasks());

        pipeline
            .all_servers()
            .into_iter()
            .try_for_each(|server_model| self.server_repo.register(server_model))?;

        scheduler.notify_pipeline_update(pipeline)
    }
}
