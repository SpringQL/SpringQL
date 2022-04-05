// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod generic_worker_thread;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue,
    task_executor::task_worker_thread_handler::TaskWorkerThreadArg,
    worker::worker_handle::{WorkerHandle, WorkerStopCoordinate},
};

use self::generic_worker_thread::GenericWorkerThread;

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorker {
    _handle: WorkerHandle,
}

impl GenericWorker {
    pub(super) fn new(
        event_queue: Arc<EventQueue>,
        worker_stop_coordinate: Arc<WorkerStopCoordinate>,
        thread_arg: TaskWorkerThreadArg,
    ) -> Self {
        let handle = WorkerHandle::new::<GenericWorkerThread>(
            event_queue,
            worker_stop_coordinate,
            thread_arg,
        );
        Self { _handle: handle }
    }
}
