// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

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
    event_queue::EventQueue,
    memory_state_machine::MemoryStateMachineThreshold,
    worker::worker_handle::{WorkerHandle, WorkerStopCoordinate},
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachineWorker {
    _handle: WorkerHandle,
}

impl MemoryStateMachineWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        memory_config: &SpringMemoryConfig,
        event_queue: Arc<EventQueue>,
        worker_stop_coordinate: Arc<WorkerStopCoordinate>,
    ) -> Self {
        let threshold = MemoryStateMachineThreshold::from(memory_config);

        let handle = WorkerHandle::new::<MemoryStateMachineWorkerThread>(
            event_queue,
            worker_stop_coordinate,
            MemoryStateMachineWorkerThreadArg::new(
                threshold,
                memory_config.memory_state_transition_interval_msec,
            ),
        );
        Self { _handle: handle }
    }
}
