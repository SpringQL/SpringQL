// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod generic_worker_pool;
mod scheduler;
mod task_executor_lock;

use crate::error::Result;
use std::sync::Arc;

use self::{
    generic_worker_pool::GenericWorkerPool,
    task_executor_lock::{PipelineUpdateLockGuard, TaskExecutorLock},
};
use super::{
    event_queue::EventQueue, performance_metrics::PerformanceMetrics,
    pipeline_derivatives::PipelineDerivatives, repositories::Repositories, task_graph::TaskGraph,
};

/// Task executor executes task graph's dataflow by internal worker threads.
/// Source tasks are scheduled by SourceScheduler and other tasks are scheduled by FlowEfficientScheduler (in Moderate state) or MemoryReducingScheduler (in Severe state).
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub(in crate::stream_engine) struct TaskExecutor {
    task_executor_lock: Arc<TaskExecutorLock>,

    repos: Arc<Repositories>,

    worker_pool: GenericWorkerPool,
}

impl TaskExecutor {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        n_worker_threads: usize,
        event_queue: Arc<EventQueue>,
        metrics: Arc<PerformanceMetrics>,
    ) -> Self {
        let task_executor_lock = Arc::new(TaskExecutorLock::default());
        let repos = Arc::new(Repositories::default());

        Self {
            task_executor_lock: task_executor_lock.clone(),

            worker_pool: GenericWorkerPool::new(
                n_worker_threads,
                event_queue,
                task_executor_lock,
                repos.clone(),
                metrics,
            ),
            repos,
        }
    }

    /// AutonomousExecutor acquires lock when pipeline is updated.
    pub(in crate::stream_engine::autonomous_executor) fn pipeline_update_lock(
        &self,
    ) -> PipelineUpdateLockGuard {
        self.task_executor_lock.pipeline_update()
    }

    /// Update workers' internal current pipeline.
    pub(in crate::stream_engine::autonomous_executor) fn update_pipeline(
        &self,
        _lock_guard: &PipelineUpdateLockGuard,
        pipeline_derivatives: Arc<PipelineDerivatives>,
    ) -> Result<()> {
        let pipeline = pipeline_derivatives.pipeline();
        pipeline
            .all_sources()
            .into_iter()
            .try_for_each(|source_reader| {
                self.repos
                    .source_reader_repository()
                    .register(source_reader)
            })?;
        pipeline
            .all_sinks()
            .into_iter()
            .try_for_each(|sink_writer| {
                self.repos.sink_writer_repository().register(sink_writer)
            })?;

        Ok(())
    }

    /// Stop all source tasks and executes pump tasks and sink tasks to finish all rows remaining in queues.
    pub(in crate::stream_engine::autonomous_executor) fn cleanup(
        &self,
        _lock_guard: &PipelineUpdateLockGuard,
        task_graph: &TaskGraph,
    ) {
        // TODO do not just remove rows in queues. Do the things in doc comment.

        self.repos
            .row_queue_repository()
            .reset(task_graph.row_queues().into_iter().collect());
    }
}
