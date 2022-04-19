// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod generic_worker_thread;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    args::Coordinators,
    event_queue::{
        blocking_event_queue::BlockingEventQueue, non_blocking_event_queue::NonBlockingEventQueue,
    },
    main_job_lock::MainJobLock,
    task_executor::task_worker_thread_handler::TaskWorkerThreadArg,
    worker::worker_handle::WorkerHandle,
};

use self::generic_worker_thread::GenericWorkerThread;

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorker {
    _handle: WorkerHandle,
}

impl GenericWorker {
    pub(super) fn new(
        main_job_lock: Arc<MainJobLock>,
        b_event_queue: Arc<BlockingEventQueue>,
        nb_event_queue: Arc<NonBlockingEventQueue>,
        coordinators: Coordinators,
        thread_arg: TaskWorkerThreadArg,
    ) -> Self {
        let handle = WorkerHandle::new::<GenericWorkerThread>(
            main_job_lock,
            b_event_queue,
            nb_event_queue,
            coordinators,
            thread_arg,
        );
        Self { _handle: handle }
    }
}
