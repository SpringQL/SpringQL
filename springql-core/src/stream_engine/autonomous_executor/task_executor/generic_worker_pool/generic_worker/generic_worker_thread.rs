// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::event::EventTag,
    performance_metrics::PerformanceMetrics,
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task::task_context::TaskContext,
    task_executor::{
        scheduler::{flow_efficient_scheduler::FlowEfficientScheduler, Scheduler},
        task_executor_lock::TaskExecutorLock,
    },
    task_graph::task_id::TaskId,
    worker::worker_thread::WorkerThread,
};

use super::generic_worker_id::GenericWorkerId;

// TODO config
const TASK_WAIT_MSEC: u64 = 100;

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct GenericWorkerThread;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct GenericWorkerThreadArg {
    id: GenericWorkerId,
    task_executor_lock: Arc<TaskExecutorLock>,
    repos: Arc<Repositories>,
}

#[derive(Debug, Default)]
pub(super) struct GenericWorkerLoopState {
    pipeline_derivatives: Arc<PipelineDerivatives>,
    metrics: Arc<PerformanceMetrics>,
    scheduler: FlowEfficientScheduler,
}

impl WorkerThread for GenericWorkerThread {
    type ThreadArg = GenericWorkerThreadArg;

    type LoopState = GenericWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState, // generic worker's loop cycle does not mutate state (while event handlers do)
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState {
        let task_executor_lock = &thread_arg.task_executor_lock;

        if let Ok(_lock) = task_executor_lock.try_task_execution() {
            let task_series = current_state.scheduler.next_task_series(
                current_state.pipeline_derivatives.task_graph(),
                current_state.metrics.as_ref(),
            );
            if !task_series.is_empty() {
                Self::execute_task_series(&task_series, &current_state, thread_arg);
            } else {
                thread::sleep(Duration::from_millis(TASK_WAIT_MSEC));
            }
        }

        current_state
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState {
        log::debug!("[GenericWorker#{}] got UpdatePipeline event", thread_arg.id);

        let mut state = current_state;

        state.pipeline_derivatives = pipeline_derivatives;

        state
    }
}

impl GenericWorkerThread {
    fn execute_task_series(
        task_series: &[TaskId],
        current_state: &GenericWorkerLoopState,
        thread_arg: &GenericWorkerThreadArg,
    ) {
        log::debug!(
            "[GenericWorker#{}] Scheduled task series:{:?}",
            thread_arg.id,
            task_series
        );

        for task_id in task_series {
            let context = TaskContext::new(
                task_id.clone(),
                current_state.pipeline_derivatives.clone(),
                thread_arg.repos.clone(),
            );

            let task = current_state
                .pipeline_derivatives
                .get_task(task_id)
                .expect("task id got from scheduler");

            task.run(&context).unwrap_or_else(Self::handle_error);
        }
    }
}
