// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod generic_worker_id;

mod worker_thread;

use std::sync::{mpsc, Arc};

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue, pipeline_derivatives::PipelineDerivatives, repositories::Repositories,
    task_executor::task_executor_lock::TaskExecutorLock,
};

use self::{generic_worker_id::GenericWorkerId, worker_thread::GenericWorkerThread};

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorker {
    stop_button: mpsc::SyncSender<()>,
}

impl GenericWorker {
    pub(super) fn new(
        id: GenericWorkerId,
        task_executor_lock: Arc<TaskExecutorLock>,
        event_queue: Arc<EventQueue>,
        repos: Arc<Repositories>,
    ) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = GenericWorkerThread::run(id, task_executor_lock, event_queue, repos, stop_receiver);

        Self { stop_button }
    }
}

impl Drop for GenericWorker {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for worker thread to finish its job");
    }
}
