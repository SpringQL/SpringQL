// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod source_worker_thread;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    args::{Coordinators, EventQueues},
    main_job_lock::MainJobLock,
    task_executor::{
        source_worker_pool::source_worker::source_worker_thread::SourceWorkerThread,
        task_worker_thread_handler::TaskWorkerThreadArg,
    },
    worker::WorkerHandle,
};

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub struct SourceWorker {
    _handle: WorkerHandle,
}

impl SourceWorker {
    pub fn new(
        main_job_lock: Arc<MainJobLock>,
        event_queues: EventQueues,
        coordinators: Coordinators,
        thread_arg: TaskWorkerThreadArg,
    ) -> Self {
        let handle = WorkerHandle::new::<SourceWorkerThread>(
            main_job_lock,
            event_queues,
            coordinators,
            thread_arg,
        );
        Self { _handle: handle }
    }
}
