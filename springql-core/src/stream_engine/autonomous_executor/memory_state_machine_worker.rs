// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Memory State Machine has 3 states: Moderate, Severe, and Critical.
//! State transition occurs when task executor's memory usage cross the threshold.
//! Threshold is calculated from memory usage upper limit configuration.
//!
//! ![Memory state machine](https://raw.githubusercontent.com/SpringQL/SpringQL/main/springql-core/doc/img/memory-state-machine-and-effect.drawio.svg)
//!
//! `TransitMemoryState` event is published on state transition and generic workers are supposed to
//! change their scheduler on Moderate and Severe state.
//! On Critical state, generic workers are stopped and purger worker cleans all rows and windows.

mod memory_state_machine_worker_thread;

use std::sync::Arc;

use crate::{
    api::SpringMemoryConfig,
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues},
        main_job_lock::MainJobLock,
        memory_state_machine::MemoryStateMachineThreshold,
        memory_state_machine_worker::memory_state_machine_worker_thread::{
            MemoryStateMachineWorkerThread, MemoryStateMachineWorkerThreadArg,
        },
        worker::WorkerHandle,
    },
};

#[derive(Debug)]
pub struct MemoryStateMachineWorker {
    _handle: WorkerHandle,
}

impl MemoryStateMachineWorker {
    pub fn new(
        memory_config: &SpringMemoryConfig,
        main_job_lock: Arc<MainJobLock>,
        event_queues: EventQueues,
        coordinators: Coordinators,
    ) -> Self {
        let threshold = MemoryStateMachineThreshold::from(memory_config);

        let handle = WorkerHandle::new::<MemoryStateMachineWorkerThread>(
            main_job_lock,
            event_queues,
            coordinators,
            MemoryStateMachineWorkerThreadArg::new(
                threshold,
                memory_config.memory_state_transition_interval_msec,
            ),
        );
        Self { _handle: handle }
    }
}
