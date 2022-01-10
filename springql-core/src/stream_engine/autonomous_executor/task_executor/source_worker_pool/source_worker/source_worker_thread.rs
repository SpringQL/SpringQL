// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::EventTag, EventQueue},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
        PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    task_executor::{
        scheduler::source_scheduler::SourceScheduler,
        task_worker_thread_handler::{
            TaskWorkerLoopState, TaskWorkerThreadArg, TaskWorkerThreadHandler,
        },
    },
    worker::worker_thread::WorkerThread,
};

use super::source_worker_id::SourceWorkerId;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct SourceWorkerThread;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct SourceWorkerThreadArg {
    id: SourceWorkerId,
    task_worker_arg: TaskWorkerThreadArg,
}

impl WorkerThread for SourceWorkerThread {
    type ThreadArg = SourceWorkerThreadArg;

    type LoopState = TaskWorkerLoopState<SourceScheduler>;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline, EventTag::ReplacePerformanceMetrics]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
        event_queue: &EventQueue,
    ) -> Self::LoopState {
        TaskWorkerThreadHandler::main_loop_cycle::<SourceScheduler>(
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
        log::debug!("[SourceWorker#{}] got UpdatePipeline event", thread_arg.id);

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
            "[SourceWorker#{}] got UpdatePerformanceMetrics event",
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
