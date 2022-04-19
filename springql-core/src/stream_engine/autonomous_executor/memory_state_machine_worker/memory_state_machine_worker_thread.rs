// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::{
        event::{Event, EventTag, NonBlockingEventTag},
        non_blocking_event_queue::NonBlockingEventQueue,
    },
    memory_state_machine::{
        MemoryStateMachine, MemoryStateMachineThreshold, MemoryStateTransition,
    },
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecutionOrPurge,
        performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    worker::{
        worker_handle::WorkerSetupCoordinator,
        worker_thread::{WorkerThread, WorkerThreadLoopState},
    },
};

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct MemoryStateMachineWorkerThread;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachineWorkerThreadArg {
    threshold: MemoryStateMachineThreshold,
    memory_state_transition_interval_msec: u32,
}

#[derive(Debug)]
pub(super) struct MemoryStateMachineWorkerLoopState {
    memory_state_machine: MemoryStateMachine,
}

impl WorkerThreadLoopState for MemoryStateMachineWorkerLoopState {
    type ThreadArg = MemoryStateMachineWorkerThreadArg;

    fn new(thread_arg: &Self::ThreadArg) -> Self
    where
        Self: Sized,
    {
        let memory_state_machine = MemoryStateMachine::new(thread_arg.threshold);
        Self {
            memory_state_machine,
        }
    }

    fn is_integral(&self) -> bool {
        true
    }
}

impl WorkerThread for MemoryStateMachineWorkerThread {
    const THREAD_NAME: &'static str = "MemoryStateMachineWorker";

    type ThreadArg = MemoryStateMachineWorkerThreadArg;

    type LoopState = MemoryStateMachineWorkerLoopState;

    fn setup_ready(worker_setup_coordinator: Arc<WorkerSetupCoordinator>) {
        worker_setup_coordinator.ready_memory_state_machine_worker()
    }

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::NonBlocking(
            NonBlockingEventTag::ReportMetricsSummary,
        )]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
        _event_queue: &NonBlockingEventQueue,
    ) -> Self::LoopState {
        // Do nothing in loop. Only curious about ReportMetricsSummary event.
        thread::sleep(Duration::from_millis(
            thread_arg.memory_state_transition_interval_msec as u64,
        ));
        current_state
    }

    fn ev_update_pipeline(
        _current_state: Self::LoopState,
        _pipeline_derivatives: Arc<PipelineDerivatives>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_replace_performance_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<PerformanceMetrics>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_incremental_update_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<MetricsUpdateByTaskExecutionOrPurge>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_report_metrics_summary(
        current_state: Self::LoopState,
        metrics_summary: Arc<PerformanceMetricsSummary>,
        _thread_arg: &Self::ThreadArg,
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        let mut state = current_state;

        let bytes = metrics_summary.queue_total_bytes;
        if let Some(transition) = state.memory_state_machine.update_memory_usage(bytes) {
            log::warn!(
                "[MemoryStateMachineWorker] Memory state transition: {:?}",
                transition
            );
            event_queue.publish(Event::TransitMemoryState {
                memory_state_transition: Arc::new(transition),
            })
        }

        state
    }

    fn ev_transit_memory_state(
        _current_state: Self::LoopState,
        _memory_state_transition: Arc<MemoryStateTransition>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }
}
