// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod source_worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue, repositories::Repositories,
    worker::worker_handle::WorkerStopCoordinate,
};

use self::source_worker::SourceWorker;

use super::{
    task_executor_lock::TaskExecutorLock,
    task_worker_thread_handler::{TaskWorkerId, TaskWorkerThreadArg},
};

/// Workers to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct SourceWorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Since worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    workers: RefCell<Vec<SourceWorker>>,
}

impl SourceWorkerPool {
    pub(super) fn new(
        n_worker_threads: u16,
        event_queue: Arc<EventQueue>,
        worker_stop_coordinate: Arc<WorkerStopCoordinate>,
        task_executor_lock: Arc<TaskExecutorLock>,
        repos: Arc<Repositories>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                let arg = TaskWorkerThreadArg::new(
                    TaskWorkerId::new(id as u16),
                    task_executor_lock.clone(),
                    repos.clone(),
                );
                SourceWorker::new(event_queue.clone(), worker_stop_coordinate.clone(), arg)
            })
            .collect();
        Self {
            workers: RefCell::new(workers),
        }
    }
}
