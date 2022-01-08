// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod generic_worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue, repositories::Repositories,
};

use self::generic_worker::{
    generic_worker_id::GenericWorkerId, generic_worker_thread::GenericWorkerThreadArg,
    GenericWorker,
};

use super::task_executor_lock::TaskExecutorLock;

/// Workers to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    workers: RefCell<Vec<GenericWorker>>,
}

impl GenericWorkerPool {
    pub(super) fn new(
        n_worker_threads: usize,
        event_queue: Arc<EventQueue>,
        task_executor_lock: Arc<TaskExecutorLock>,
        repos: Arc<Repositories>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                let arg = GenericWorkerThreadArg::new(
                    GenericWorkerId::new(id as u16),
                    task_executor_lock.clone(),
                    repos.clone(),
                );
                GenericWorker::new(event_queue.clone(), arg)
            })
            .collect();
        Self {
            workers: RefCell::new(workers),
        }
    }
}
