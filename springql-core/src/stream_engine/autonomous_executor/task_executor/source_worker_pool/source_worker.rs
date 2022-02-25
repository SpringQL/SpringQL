// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod source_worker_thread;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue,
    task_executor::task_worker_thread_handler::TaskWorkerThreadArg,
    worker::worker_handle::{WorkerHandle, WorkerStopCoordinate},
};

use self::source_worker_thread::SourceWorkerThread;

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct SourceWorker {
    _handle: WorkerHandle,
}

impl SourceWorker {
    pub(super) fn new(
        event_queue: Arc<EventQueue>,
        worker_stop_coordinate: Arc<WorkerStopCoordinate>,
        thread_arg: TaskWorkerThreadArg,
    ) -> Self {
        let handle = WorkerHandle::new::<SourceWorkerThread>(
            event_queue,
            worker_stop_coordinate,
            thread_arg,
        );
        Self { _handle: handle }
    }
}
