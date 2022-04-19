// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Memory State Machine has 3 states: Moderate, Severe, and Critical.
//! State transition occurs when task executor's memory usage cross the threshold.
//! Threshold is calculated from memory usage upper limit configuration.
//!
//! ![Memory state machine](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/memory-state-machine-and-effect.svg)
//!
//! `TransitMemoryState` event is published on state transition and generic workers are supposed to
//! change their scheduler on Moderate and Severe state.
//! On Critical state, generic workers are stopped and purger worker cleans all rows and windows.

pub(in crate::stream_engine::autonomous_executor) mod memory_state_machine_worker_thread;

use std::sync::Arc;

use crate::low_level_rs::SpringMemoryConfig;

use self::memory_state_machine_worker_thread::{
    MemoryStateMachineWorkerThread, MemoryStateMachineWorkerThreadArg,
};

use super::{
    args::Coordinators,
    event_queue::{
        blocking_event_queue::BlockingEventQueue, non_blocking_event_queue::NonBlockingEventQueue,
    },
    main_job_lock::MainJobLock,
    memory_state_machine::MemoryStateMachineThreshold,
    worker::worker_handle::WorkerHandle,
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachineWorker {
    _handle: WorkerHandle,
}

impl MemoryStateMachineWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        memory_config: &SpringMemoryConfig,
        main_job_lock: Arc<MainJobLock>,
        b_event_queue: Arc<BlockingEventQueue>,
        nb_event_queue: Arc<NonBlockingEventQueue>,
        coordinators: Coordinators,
    ) -> Self {
        let threshold = MemoryStateMachineThreshold::from(memory_config);

        let handle = WorkerHandle::new::<MemoryStateMachineWorkerThread>(
            main_job_lock,
            b_event_queue,
            nb_event_queue,
            coordinators,
            MemoryStateMachineWorkerThreadArg::new(
                threshold,
                memory_config.memory_state_transition_interval_msec,
            ),
        );
        Self { _handle: handle }
    }
}
