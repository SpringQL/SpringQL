// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives, repositories::Repositories,
};

use self::worker::{worker_id::WorkerId, Worker};

use super::task_executor_lock::TaskExecutorLock;

#[derive(Debug)]
pub(super) struct WorkerPool {
    /// Worker pool gets interruption from task executor on, for example, pipeline update.
    /// Worker pool holder cannot always be mutable, worker pool is better to have mutability for each worker.
    ///
    /// Mutation to workers only happens inside task executor lock like `PipelineUpdateLockGuard`,
    /// so here uses RefCell instead of Mutex nor RwLock to avoid lock cost to workers.
    workers: RefCell<Vec<Worker>>,
}

impl WorkerPool {
    pub(super) fn new(
        n_worker_threads: usize,
        task_executor_lock: Arc<TaskExecutorLock>,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        repos: Arc<Repositories>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                Worker::new(
                    WorkerId::new(id as u16),
                    task_executor_lock.clone(),
                    pipeline_derivatives.clone(),
                    repos.clone(),
                )
            })
            .collect();
        Self {
            workers: RefCell::new(workers),
        }
    }

    /// Interruption from task executor to update worker's pipeline.
    pub(super) fn interrupt_pipeline_update(&self, pipeline_derivatives: Arc<PipelineDerivatives>) {
        for worker in self.workers.borrow_mut().iter_mut() {
            worker.interrupt_pipeline_update(pipeline_derivatives.clone());
        }
    }
}
