// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(super) mod generic_worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue,
    main_job_lock::MainJobLock,
    repositories::Repositories,
    worker::worker_handle::{WorkerSetupCoordinator, WorkerStopCoordinator},
};

use self::generic_worker::GenericWorker;

use super::{
    task_executor_lock::TaskExecutorLock,
    task_worker_thread_handler::{TaskWorkerId, TaskWorkerThreadArg},
};

/// Workers to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Since worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    _workers: RefCell<Vec<GenericWorker>>,
}

impl GenericWorkerPool {
    pub(super) fn new(
        n_worker_threads: u16,
        main_job_lock: Arc<MainJobLock>,
        event_queue: Arc<EventQueue>,
        worker_setup_coordinator: Arc<WorkerSetupCoordinator>,
        worker_stop_coordinator: Arc<WorkerStopCoordinator>,
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
                GenericWorker::new(
                    main_job_lock.clone(),
                    event_queue.clone(),
                    worker_setup_coordinator.clone(),
                    worker_stop_coordinator.clone(),
                    arg,
                )
            })
            .collect();
        Self {
            _workers: RefCell::new(workers),
        }
    }
}
