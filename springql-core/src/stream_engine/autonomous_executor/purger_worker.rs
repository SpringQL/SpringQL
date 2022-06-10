// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod purger_worker_thread;

pub use purger_worker_thread::{PurgerWorkerLoopState, PurgerWorkerThread, PurgerWorkerThreadArg};

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    args::{Coordinators, EventQueues},
    main_job_lock::MainJobLock,
    worker::WorkerHandle,
};

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub struct PurgerWorker {
    _handle: WorkerHandle,
}

impl PurgerWorker {
    pub fn new(
        main_job_lock: Arc<MainJobLock>,
        event_queues: EventQueues,
        coordinators: Coordinators,
        thread_arg: PurgerWorkerThreadArg,
    ) -> Self {
        let handle = WorkerHandle::new::<PurgerWorkerThread>(
            main_job_lock,
            event_queues,
            coordinators,
            thread_arg,
        );
        Self { _handle: handle }
    }
}
