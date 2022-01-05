// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod worker;

use std::{cell::RefCell, sync::Arc};

use crate::stream_engine::autonomous_executor::{
    current_pipeline::CurrentPipeline,
    row::row_repository::RowRepository,
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
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
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<RowRepository>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                Worker::new(
                    WorkerId::new(id as u16),
                    task_executor_lock.clone(),
                    current_pipeline.clone(),
                    row_repo.clone(),
                    source_reader_repo.clone(),
                    sink_writer_repo.clone(),
                )
            })
            .collect();
        Self {
            workers: RefCell::new(workers),
        }
    }

    /// Interruption from task executor to update worker's pipeline.
    pub(super) fn interrupt_pipeline_update(&self, current_pipeline: Arc<CurrentPipeline>) {
        for worker in self.workers.borrow_mut().iter_mut() {
            worker.interrupt_pipeline_update(current_pipeline.clone());
        }
    }
}
