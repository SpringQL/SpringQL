// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod worker_id;

mod worker_thread;

use std::sync::{mpsc, Arc};

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives,
    row::row_repository::RowRepository,
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
    task_executor::task_executor_lock::TaskExecutorLock,
};

use self::{worker_id::WorkerId, worker_thread::WorkerThread};

#[derive(Debug)]
pub(super) struct Worker {
    pipeline_update_signal: mpsc::SyncSender<Arc<PipelineDerivatives>>,
    stop_button: mpsc::SyncSender<()>,
}

impl Worker {
    pub(super) fn new(
        id: WorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        row_repo: Arc<RowRepository>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        let (pipeline_update_signal, pipeline_update_receiver) = mpsc::sync_channel(0);
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = WorkerThread::run(
            id,
            task_executor_lock,
            pipeline_derivatives,
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
    pub(super) fn interrupt_pipeline_update(
        &mut self,
        pipeline_derivatives: Arc<PipelineDerivatives>,
    ) {
        self.pipeline_update_signal
            .send(pipeline_derivatives)
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
