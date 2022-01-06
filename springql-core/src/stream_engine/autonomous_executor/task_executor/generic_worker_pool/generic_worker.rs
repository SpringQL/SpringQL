// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod generic_worker_id;

mod worker_thread;

use std::sync::{mpsc, Arc};

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives, repositories::Repositories,
    task_executor::task_executor_lock::TaskExecutorLock,
};

use self::{generic_worker_id::GenericWorkerId, worker_thread::GenericWorkerThread};

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorker {
    pipeline_update_signal: mpsc::SyncSender<Arc<PipelineDerivatives>>,
    stop_button: mpsc::SyncSender<()>,
}

impl GenericWorker {
    pub(super) fn new(
        id: GenericWorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        repos: Arc<Repositories>,
    ) -> Self {
        let (pipeline_update_signal, pipeline_update_receiver) = mpsc::sync_channel(0);
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = GenericWorkerThread::run(
            id,
            task_executor_lock,
            pipeline_derivatives,
            repos,
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

impl Drop for GenericWorker {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for worker thread to finish its job");
    }
}
