// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine) mod task;

pub(crate) mod row;

mod scheduler;
mod worker_pool;

use crate::{error::Result, pipeline::Pipeline};
use std::sync::{Arc, RwLock};

pub(crate) use row::ForeignSinkRow;
pub(in crate::stream_engine) use row::{
    CurrentTimestamp, NaiveRowRepository, RowRepository, Timestamp,
};
pub(in crate::stream_engine) use scheduler::{FlowEfficientScheduler, Scheduler};

use self::{
    scheduler::{scheduler_read::SchedulerRead, scheduler_write::SchedulerWrite},
    task::source_task::{
        sink_subtask::sink_subtask_repository::SinkSubtaskRepository,
        source_subtask::source_subtask_repository::SourceSubtaskRepository,
    },
    worker_pool::WorkerPool,
};

use super::dependency_injection::DependencyInjection;

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
    scheduler_write: SchedulerWrite<DI>,

    row_repo: Arc<DI::RowRepositoryType>,
    source_subtask_repo: Arc<SourceSubtaskRepository>,
    sink_subtask_repo: Arc<SinkSubtaskRepository>,

    #[allow(unused)] // not referenced but just holding ownership to make workers continuously run
    worker_pool: WorkerPool,
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
        let source_subtask_repo = Arc::new(SourceSubtaskRepository::default());
        let sink_subtask_repo = Arc::new(SinkSubtaskRepository::default());

        Self {
            scheduler_write,

            worker_pool: WorkerPool::new::<DI>(
                n_worker_threads,
                scheduler_read,
                row_repo.clone(),
                source_subtask_repo.clone(),
                sink_subtask_repo.clone(),
            ),
            row_repo,
            source_subtask_repo,
            sink_subtask_repo,
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
            .all_sources()
            .into_iter()
            .try_for_each(|server_model| self.source_subtask_repo.register(server_model))?;
        pipeline
            .all_sinks()
            .into_iter()
            .try_for_each(|server_model| self.sink_subtask_repo.register(server_model))?;

        scheduler.notify_pipeline_update(pipeline)
    }
}
