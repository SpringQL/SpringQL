// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(super) mod source_worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    args::{Coordinators, Locks},
    event_queue::{
        blocking_event_queue::BlockingEventQueue, non_blocking_event_queue::NonBlockingEventQueue,
    },
    repositories::Repositories,
};

use self::source_worker::SourceWorker;

use super::task_worker_thread_handler::{TaskWorkerId, TaskWorkerThreadArg};

/// Workers to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct SourceWorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Since worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    _workers: RefCell<Vec<SourceWorker>>,
}

impl SourceWorkerPool {
    pub(super) fn new(
        n_worker_threads: u16,
        locks: Locks,
        b_event_queue: Arc<BlockingEventQueue>,
        nb_event_queue: Arc<NonBlockingEventQueue>,
        coordinators: Coordinators,
        repos: Arc<Repositories>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                let arg = TaskWorkerThreadArg::new(
                    TaskWorkerId::new(id as u16),
                    locks.task_executor_lock.clone(),
                    repos.clone(),
                );
                SourceWorker::new(
                    locks.main_job_lock.clone(),
                    b_event_queue.clone(),
                    nb_event_queue.clone(),
                    coordinators.clone(),
                    arg,
                )
            })
            .collect();
        Self {
            _workers: RefCell::new(workers),
        }
    }
}
