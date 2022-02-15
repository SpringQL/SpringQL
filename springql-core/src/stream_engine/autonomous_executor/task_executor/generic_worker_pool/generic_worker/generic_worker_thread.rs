// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod generic_worker_scheduler;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::EventTag, EventQueue},
    memory_state_machine::{MemoryState, MemoryStateTransition},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
        performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    task_executor::task_worker_thread_handler::{
        TaskWorkerLoopState, TaskWorkerThreadArg, TaskWorkerThreadHandler,
    },
    worker::worker_thread::WorkerThread,
};

use self::generic_worker_scheduler::GenericWorkerScheduler;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct GenericWorkerThread;

impl WorkerThread for GenericWorkerThread {
    const THREAD_NAME: &'static str = "GenericWorker";

    type ThreadArg = TaskWorkerThreadArg;

    type LoopState = TaskWorkerLoopState<GenericWorkerScheduler>;

    fn event_subscription() -> Vec<EventTag> {
        vec![
            EventTag::UpdatePipeline,
            EventTag::ReplacePerformanceMetrics,
            EventTag::TransitMemoryState,
        ]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState, // generic worker's loop cycle does not mutate state (while event handlers do)
        thread_arg: &Self::ThreadArg,
        event_queue: &EventQueue,
    ) -> Self::LoopState {
        TaskWorkerThreadHandler::main_loop_cycle::<GenericWorkerScheduler>(
            current_state,
            thread_arg,
            event_queue,
        )
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        log::debug!(
            "[GenericWorker#{}] got UpdatePipeline event",
            thread_arg.worker_id
        );

        let mut state = current_state;
        state.pipeline_derivatives = Some(pipeline_derivatives);
        state
    }

    fn ev_replace_performance_metrics(
        current_state: Self::LoopState,
        metrics: Arc<PerformanceMetrics>,
        thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        log::debug!(
            "[GenericWorker#{}] got ReplacePerformanceMetrics event",
            thread_arg.worker_id
        );

        let mut state = current_state;
        state.metrics = Some(metrics);
        state
    }

    fn ev_transit_memory_state(
        current_state: Self::LoopState,
        memory_state_transition: Arc<MemoryStateTransition>,
        thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        let mut state = current_state;

        match memory_state_transition.to_state() {
            MemoryState::Moderate => {
                log::info!(
                    "[GenericWorker#{}] Switched to FlowEfficientScheduler",
                    thread_arg.worker_id
                );
                state.scheduler = GenericWorkerScheduler::flow_efficient_scheduler();
            }
            MemoryState::Severe => {
                log::info!(
                    "[GenericWorker#{}] Switched to MemoryReducingScheduler",
                    thread_arg.worker_id
                );
                state.scheduler = GenericWorkerScheduler::memory_reducing_scheduler();
            }
            MemoryState::Critical => log::info!(
                "[GenericWorker#{}] Wait for PurgerWorker to transit memory state",
                thread_arg.worker_id
            ),
        }

        state
    }

    fn ev_incremental_update_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<MetricsUpdateByTaskExecution>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_report_metrics_summary(
        _current_state: Self::LoopState,
        _metrics_summary: Arc<PerformanceMetricsSummary>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }
}
