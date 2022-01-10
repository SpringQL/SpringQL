use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::EventTag, EventQueue},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
        PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    worker::worker_thread::{WorkerThread, WorkerThreadLoopState},
};

// TODO config
const CLOCK_MSEC: u64 = 100;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct MemoryStateMachineWorkerThread;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachineWorkerThreadArg {
    threshold: MemoryStateMachineThreshold,
}

#[derive(Debug)]
pub(super) struct MemoryStateMachineWorkerLoopState {
    memory_state: MemoryState,
}

impl WorkerThreadLoopState for MemoryStateMachineWorkerLoopState {
    fn is_integral(&self) -> bool {
        true
    }
}

impl WorkerThread for MemoryStateMachineWorkerThread {
    type ThreadArg = MemoryStateMachineWorkerThreadArg;

    type LoopState = MemoryStateMachineWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::ReportMetricsSummary]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        _thread_arg: &Self::ThreadArg,
        _event_queue: &EventQueue,
    ) -> Self::LoopState {
        // Do nothing in loop. Only curious about ReportMetricsSummary event.
        thread::sleep(Duration::from_millis(CLOCK_MSEC));
        current_state
    }

    fn ev_update_pipeline(
        _current_state: Self::LoopState,
        _pipeline_derivatives: Arc<PipelineDerivatives>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_replace_performance_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<PerformanceMetrics>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_incremental_update_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<MetricsUpdateByTaskExecution>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }
}
