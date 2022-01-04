// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod worker_id;

mod worker_thread;

use std::sync::{mpsc, Arc};

use crate::stream_engine::{
    autonomous_executor::{
        current_pipeline::CurrentPipeline,
        task::{
            sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
            source_task::source_reader::source_reader_repository::SourceReaderRepository,
        },
        task_executor::task_executor_lock::TaskExecutorLock,
    },
    dependency_injection::DependencyInjection,
};

use self::{worker_id::WorkerId, worker_thread::WorkerThread};

#[derive(Debug)]
pub(super) struct Worker {
    pipeline_update_signal: mpsc::SyncSender<Arc<CurrentPipeline>>,
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new<DI: DependencyInjection>(
        id: WorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        let (pipeline_update_signal, pipeline_update_receiver) = mpsc::sync_channel(0);
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = WorkerThread::run::<DI>(
            id,
            task_executor_lock,
            current_pipeline,
            row_repo,
            source_reader_repo,
            sink_writer_repo,
            pipeline_update_receiver,
            stop_receiver,
        );

        Self {
            pipeline_update_signal,
            stop_button,
        }
    }

    /// Interruption from task executor to update worker's pipeline.
    pub(super) fn interrupt_pipeline_update(&mut self, current_pipeline: Arc<CurrentPipeline>) {
        self.pipeline_update_signal
            .send(current_pipeline)
            .expect("failed to send new pipeline to worker thread");
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for worker thread to finish its job");
    }
}
