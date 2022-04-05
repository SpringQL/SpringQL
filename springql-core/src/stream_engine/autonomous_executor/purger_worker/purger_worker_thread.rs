// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::{
        event::{Event, EventTag},
        EventQueue,
    },
    memory_state_machine::{MemoryState, MemoryStateTransition},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecutionOrPurge,
        performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task_executor::task_executor_lock::TaskExecutorLock,
    worker::worker_thread::{WorkerThread, WorkerThreadLoopState},
};

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct PurgerWorkerThreadArg {
    repos: Arc<Repositories>,
    task_executor_lock: Arc<TaskExecutorLock>,
}

#[derive(Debug)]
pub(super) struct PurgerWorkerLoopState {
    pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
}
impl WorkerThreadLoopState for PurgerWorkerLoopState {
    type ThreadArg = PurgerWorkerThreadArg;

    fn new(_thread_arg: &Self::ThreadArg) -> Self
    where
        Self: Sized,
    {
        Self {
            pipeline_derivatives: None,
        }
    }

    fn is_integral(&self) -> bool {
        true
    }
}

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct PurgerWorkerThread;

impl WorkerThread for PurgerWorkerThread {
    const THREAD_NAME: &'static str = "PurgerWorker";

    type ThreadArg = PurgerWorkerThreadArg;

    type LoopState = PurgerWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline, EventTag::TransitMemoryState]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        _thread_arg: &Self::ThreadArg,
        _event_queue: &EventQueue,
    ) -> Self::LoopState {
        // Do nothing in loop. Only curious about TransitMemoryState event.
        thread::sleep(Duration::from_millis(100));
        current_state
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        log::debug!("[PurgerWorker] got UpdatePipeline event",);

        let mut state = current_state;
        state.pipeline_derivatives = Some(pipeline_derivatives);
        state
    }

    fn ev_transit_memory_state(
        current_state: Self::LoopState,
        memory_state_transition: Arc<MemoryStateTransition>,
        thread_arg: &Self::ThreadArg,
        event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        match memory_state_transition.to_state() {
            MemoryState::Moderate | MemoryState::Severe => {
                // do nothing
            }
            MemoryState::Critical => {
                log::warn!("[PurgerWorker] Start purging...",);

                let task_executor_lock = &thread_arg.task_executor_lock;
                let _lock = task_executor_lock.task_execution_barrier();

                // purge queues
                let row_queue_repo = thread_arg.repos.row_queue_repository();
                let window_queue_repo = thread_arg.repos.window_queue_repository();
                row_queue_repo.purge();
                window_queue_repo.purge();

                // purge windows
                if let Some(pd) = &current_state.pipeline_derivatives {
                    let task_repo = pd.task_repo();
                    task_repo.purge_windows()
                }

                // reset metrics
                event_queue.publish(Event::IncrementalUpdateMetrics {
                    metrics_update_by_task_execution_or_purge: Arc::new(
                        MetricsUpdateByTaskExecutionOrPurge::Purge,
                    ),
                });

                log::warn!("[PurgerWorker] Finished purging. Sent `MetricsUpdateByTaskExecutionOrPurge::Purge` event.");
            }
        }

        current_state
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
        _metrics: Arc<MetricsUpdateByTaskExecutionOrPurge>,
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
