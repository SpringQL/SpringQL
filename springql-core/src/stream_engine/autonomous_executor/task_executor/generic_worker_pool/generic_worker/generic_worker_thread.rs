// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod generic_worker_scheduler;

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::EventTag, EventQueue},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
        PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    task_executor::{
        scheduler::{
            flow_efficient_scheduler::FlowEfficientScheduler,
            memory_reducing_scheduler::MemoryReducingScheduler,
        },
        task_worker_thread_handler::{
            TaskWorkerLoopState, TaskWorkerThreadArg, TaskWorkerThreadHandler,
        },
    },
    worker::worker_thread::WorkerThread,
};

use self::generic_worker_scheduler::GenericWorkerScheduler;

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

    type LoopState = TaskWorkerLoopState<GenericWorkerScheduler>;

    fn event_subscription() -> Vec<EventTag> {
        vec![
            EventTag::UpdatePipeline,
            EventTag::ReplacePerformanceMetrics,
        ]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState, // generic worker's loop cycle does not mutate state (while event handlers do)
        thread_arg: &Self::ThreadArg,
        event_queue: &EventQueue,
    ) -> Self::LoopState {
        TaskWorkerThreadHandler::main_loop_cycle::<GenericWorkerScheduler>(
            current_state,
            &thread_arg.task_worker_arg,
            event_queue,
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
            "[GenericWorker#{}] got UpdatePerformanceMetrics event",
            thread_arg.id
        );

        let mut state = current_state;
        state.metrics = Some(metrics);
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
}
