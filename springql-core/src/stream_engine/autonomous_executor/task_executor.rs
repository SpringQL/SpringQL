// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod scheduler;
mod task_executor_lock;
mod worker_pool;

use crate::{error::Result, stream_engine::dependency_injection::DependencyInjection};
use std::sync::{Arc, RwLock};

pub(in crate::stream_engine) use super::row::RowRepository;
pub(in crate::stream_engine) use scheduler::{FlowEfficientScheduler, Scheduler};

use self::{
    scheduler::{scheduler_read::SchedulerRead, scheduler_write::SchedulerWrite},
    task_executor_lock::{PipelineUpdateLockGuard, TaskExecutorLock},
    worker_pool::WorkerPool,
};
use super::{
    current_pipeline::CurrentPipeline,
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
};

/// Task executor executes task graph's dataflow by internal worker threads.
/// Source tasks are scheduled by SourceScheduler and other tasks are scheduled by FlowEfficientScheduler (in Moderate state) or MemoryReducingScheduler (in Severe state).
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct TaskExecutor<DI>
where
    DI: DependencyInjection,
{
    task_executor_lock: Arc<TaskExecutorLock>,

    current_pipeline: Arc<CurrentPipeline>,

    scheduler_write: SchedulerWrite<DI>,

    row_repo: Arc<DI::RowRepositoryType>,
    source_reader_repo: Arc<SourceReaderRepository>,
    sink_writer_repo: Arc<SinkWriterRepository>,

    _worker_pool: WorkerPool, // not referenced but just holding ownership to make workers continuously run
}

impl<DI> TaskExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine::autonomous_executor) fn new(
        n_worker_threads: usize,
        current_pipeline: Arc<CurrentPipeline>,
    ) -> Self {
        let task_executor_lock = Arc::new(TaskExecutorLock::default());

        let scheduler = Arc::new(RwLock::new(DI::SchedulerType::default()));
        let scheduler_write = SchedulerWrite::new(scheduler.clone());
        let scheduler_read = SchedulerRead::new(scheduler);

        let row_repo = Arc::new(DI::RowRepositoryType::default());
        let source_reader_repo = Arc::new(SourceReaderRepository::default());
        let sink_writer_repo = Arc::new(SinkWriterRepository::default());

        Self {
            task_executor_lock: task_executor_lock.clone(),

            current_pipeline: current_pipeline.clone(),

            scheduler_write,

            _worker_pool: WorkerPool::new::<DI>(
                n_worker_threads,
                task_executor_lock,
                current_pipeline,
                scheduler_read,
                row_repo.clone(),
                source_reader_repo.clone(),
                sink_writer_repo.clone(),
            ),
            row_repo,
            source_reader_repo,
            sink_writer_repo,
        }
    }

    /// AutonomousExecutor acquires lock when pipeline is updated.
    pub(in crate::stream_engine::autonomous_executor) fn pipeline_update_lock(
        &self,
    ) -> PipelineUpdateLockGuard {
        self.task_executor_lock.pipeline_update()
    }

    /// Stop all source tasks and executes pump tasks and sink tasks to finish all rows remaining in queues.
    pub(in crate::stream_engine::autonomous_executor) fn cleanup(
        &self,
        _lock_guard: &PipelineUpdateLockGuard,
    ) -> Result<()> {
        // TODO do not just remove rows in queues. Do the things in doc comment.

        let inner = self.current_pipeline.read();
        let pipeline = inner.pipeline();
        let task_graph = inner.task_graph();

        self.row_repo.reset(task_graph.all_tasks());

        pipeline
            .all_sources()
            .into_iter()
            .try_for_each(|source_reader| self.source_reader_repo.register(source_reader))?;
        pipeline
            .all_sinks()
            .into_iter()
            .try_for_each(|sink_writer| self.sink_writer_repo.register(sink_writer))?;

        let mut scheduler = self.scheduler_write.write_lock();
        scheduler.notify_pipeline_update(self.current_pipeline.as_ref())
    }
}
