// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod source_worker;

use std::{cell::RefCell, sync::Arc};

use crate::{
    api::SpringWorkerConfig,
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues, Locks},
        repositories::Repositories,
        task_executor::{
            source_worker_pool::source_worker::SourceWorker,
            task_worker_thread_handler::{TaskWorkerId, TaskWorkerThreadArg},
        },
    },
};

/// Workers to execute pump and sink tasks.
#[derive(Debug)]
pub struct SourceWorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Since worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    _workers: RefCell<Vec<SourceWorker>>,
}

impl SourceWorkerPool {
    pub fn new(
        config: &SpringWorkerConfig,
        locks: Locks,
        event_queues: EventQueues,
        coordinators: Coordinators,
        repos: Arc<Repositories>,
    ) -> Self {
        let workers = (0..config.n_source_worker_threads)
            .map(|id| {
                let arg = TaskWorkerThreadArg::new(
                    TaskWorkerId::new(id),
                    locks.task_executor_lock.clone(),
                    repos.clone(),
                    config.sleep_msec_no_row,
                );
                SourceWorker::new(
                    locks.main_job_lock.clone(),
                    event_queues.clone(),
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
