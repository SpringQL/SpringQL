// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::EventTag, EventQueue},
    performance_metrics::PerformanceMetrics,
    pipeline_derivatives::PipelineDerivatives,
    task_executor::{
        scheduler::flow_efficient_scheduler::FlowEfficientScheduler,
        task_worker_thread_handler::{
            TaskWorkerLoopState, TaskWorkerThreadArg, TaskWorkerThreadHandler,
        },
    },
    worker::worker_thread::WorkerThread,
};

use super::generic_worker_id::GenericWorkerId;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct GenericWorkerThread;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct GenericWorkerThreadArg {
    id: GenericWorkerId,
    task_worker_arg: TaskWorkerThreadArg,
}

impl WorkerThread for GenericWorkerThread {
    type ThreadArg = GenericWorkerThreadArg;

    type LoopState = TaskWorkerLoopState<FlowEfficientScheduler>; // TODO make `enum SwitchScheduler`

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline, EventTag::UpdatePerformanceMetrics]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState, // generic worker's loop cycle does not mutate state (while event handlers do)
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState {
        TaskWorkerThreadHandler::main_loop_cycle::<Self, FlowEfficientScheduler>(
            current_state,
            &thread_arg.task_worker_arg,
        )
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        log::debug!("[GenericWorker#{}] got UpdatePipeline event", thread_arg.id);

        let mut state = current_state;
        state.pipeline_derivatives = pipeline_derivatives;
        state
    }

    fn ev_update_performance_metrics(
        current_state: Self::LoopState,
        metrics: Arc<PerformanceMetrics>,
        thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        log::debug!(
            "[GenericWorker#{}] got UpdatePerformanceMetrics event",
            thread_arg.id
        );

        let mut state = current_state;
        state.metrics = metrics;
        state
    }
}
