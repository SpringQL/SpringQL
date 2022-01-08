// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod generic_worker_id;
pub(in crate::stream_engine::autonomous_executor) mod generic_worker_thread;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::EventQueue, worker::worker_handle::WorkerHandle,
};

use self::generic_worker_thread::{GenericWorkerThread, GenericWorkerThreadArg};

/// Worker to execute pump and sink tasks.
#[derive(Debug)]
pub(super) struct GenericWorker {
    handle: WorkerHandle,
}

impl GenericWorker {
    pub(super) fn new(event_queue: Arc<EventQueue>, thread_arg: GenericWorkerThreadArg) -> Self {
        let handle = WorkerHandle::new::<GenericWorkerThread>(event_queue, thread_arg);
        Self { handle }
    }
}
