// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::event::EventTag,
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task::task_context::TaskContext,
    task_executor::{
        scheduler::{flow_efficient_scheduler::FlowEfficientScheduler, Scheduler},
        task_executor_lock::TaskExecutorLock,
    },
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
    scheduler: FlowEfficientScheduler,
    cur_worker_state: <FlowEfficientScheduler as Scheduler>::W,
}

impl WorkerThread for GenericWorkerThread {
    type ThreadArg = GenericWorkerThreadArg;

    type LoopState = GenericWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState {
        let id = thread_arg.id;
        let task_executor_lock = &thread_arg.task_executor_lock;
        let repos = &thread_arg.repos;

        let mut state = current_state;

        if let Ok(_lock) = task_executor_lock.try_task_execution() {
            if let Some((task_id, next_worker_state)) =
                state.scheduler.next_task(state.cur_worker_state)
            {
                log::debug!("[GenericWorker#{}] Scheduled task:{}", id, task_id);

                state.cur_worker_state = next_worker_state;

                let context = TaskContext::new(
                    task_id.clone(),
                    state.pipeline_derivatives.clone(),
                    repos.clone(),
                );

                let task = state
                    .pipeline_derivatives
                    .get_task(&task_id)
                    .expect("task id got from scheduler");

                task.run(&context).unwrap_or_else(Self::handle_error);
            } else {
                thread::sleep(Duration::from_millis(TASK_WAIT_MSEC));
            }
        }

        state
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState {
        log::debug!("[GenericWorker#{}] got UpdatePipeline event", thread_arg.id);

        let mut state = current_state;

        state
            .scheduler
            .notify_pipeline_update(pipeline_derivatives.as_ref())
            .expect("failed to update scheduler's state");

        state.pipeline_derivatives = pipeline_derivatives;

        state
    }
}
