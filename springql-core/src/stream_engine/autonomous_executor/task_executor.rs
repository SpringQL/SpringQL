// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod generic_worker_pool;
mod scheduler;
mod source_worker_pool;
mod task_executor_lock;
mod task_worker_thread_handler;

pub use task_executor_lock::{
    TaskExecutionBarrierGuard, TaskExecutionLockGuard, TaskExecutorLock, TaskExecutorLockToken,
};

use std::sync::Arc;

use crate::{
    api::{error::Result, SpringConfig},
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues, Locks},
        main_job_lock::MainJobBarrierGuard,
        pipeline_derivatives::PipelineDerivatives,
        repositories::Repositories,
        task_executor::{
            generic_worker_pool::GenericWorkerPool, source_worker_pool::SourceWorkerPool,
        },
        task_graph::TaskGraph,
    },
};

/// Task executor executes task graph's dataflow by internal worker threads.
/// Source tasks are scheduled by SourceScheduler and other tasks are scheduled by FlowEfficientScheduler (in Moderate state) or MemoryReducingScheduler (in Severe state).
///
/// All interface methods are called from main thread, while `new()` spawns worker threads.
#[derive(Debug)]
pub struct TaskExecutor {
    repos: Arc<Repositories>,

    _generic_worker_pool: GenericWorkerPool,
    _source_worker_pool: SourceWorkerPool,
}

impl TaskExecutor {
    pub fn new(
        config: &SpringConfig,
        repos: Arc<Repositories>,
        locks: Locks,
        event_queues: EventQueues,
        coordinators: Coordinators,
    ) -> Self {
        Self {
            repos: repos.clone(),

            _generic_worker_pool: GenericWorkerPool::new(
                &config.worker,
                locks.clone(),
                event_queues.clone(),
                coordinators.clone(),
                repos.clone(),
            ),
            _source_worker_pool: SourceWorkerPool::new(
                &config.worker,
                locks,
                event_queues,
                coordinators,
                repos,
            ),
        }
    }

    /// Update workers' internal current pipeline.
    pub fn update_pipeline(
        &self,
        _lock_guard: &MainJobBarrierGuard,
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
    pub fn cleanup(&self, _lock_guard: &MainJobBarrierGuard, task_graph: &TaskGraph) {
        // TODO do not just remove rows in queues. Do the things in doc comment.

        self.repos
            .row_queue_repository()
            .reset(task_graph.row_queues().into_iter().collect());
        self.repos
            .window_queue_repository()
            .reset(task_graph.window_queues().into_iter().collect());
    }
}
