// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    sync::{mpsc, Arc},
    thread,
};

use crate::stream_engine::autonomous_executor::{
    event_queue::EventPoll,
    main_job_lock::MainJobLock,
    memory_state_machine::MemoryStateTransition,
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecutionOrPurge,
        performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
    },
};

use crate::stream_engine::autonomous_executor::{
    event_queue::{
        event::{Event, EventTag},
        non_blocking_event_queue::NonBlockingEventQueue,
    },
    pipeline_derivatives::PipelineDerivatives,
};

use super::worker_handle::{WorkerSetupCoordinator, WorkerStopCoordinator};

/// State updated by loop cycles and event handlers.
pub(in crate::stream_engine::autonomous_executor) trait WorkerThreadLoopState {
    /// Supposed to be same type as `WorkerThread::ThreadArg`.
    ///
    /// Use `()` if no arg needed.
    type ThreadArg: Send + 'static;

    /// Initial state.
    fn new(thread_arg: &Self::ThreadArg) -> Self
    where
        Self: Sized;

    /// State might become partially corrupt when, for example, an event is subscribed but another related event is not.
    /// State itself must provide integrity check by this method.
    fn is_integral(&self) -> bool;
}

pub(in crate::stream_engine::autonomous_executor) trait WorkerThread {
    const THREAD_NAME: &'static str;

    /// Immutable argument to pass to `main_loop_cycle`.
    ///
    /// Use `()` if no arg needed.
    type ThreadArg: Send + 'static;

    /// State updated in each `main_loop_cycle`.
    ///
    /// Use `()` if no state needed.
    type LoopState: WorkerThreadLoopState<ThreadArg = Self::ThreadArg>;

    /// Which events to subscribe
    fn event_subscription() -> Vec<EventTag>;

    /// A cycle in `main_loop`
    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,

        // for a cycle to push event
        event_queue: &NonBlockingEventQueue,
    ) -> Self::LoopState;

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &Self::ThreadArg,

        // for cascading event
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState;

    fn ev_replace_performance_metrics(
        current_state: Self::LoopState,
        metrics: Arc<PerformanceMetrics>,
        thread_arg: &Self::ThreadArg,

        // for cascading event
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState;

    fn ev_incremental_update_metrics(
        current_state: Self::LoopState,
        metrics: Arc<MetricsUpdateByTaskExecutionOrPurge>,
        thread_arg: &Self::ThreadArg,

        // for cascading event
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState;

    fn ev_report_metrics_summary(
        current_state: Self::LoopState,
        metrics_summary: Arc<PerformanceMetricsSummary>,
        thread_arg: &Self::ThreadArg,

        // for cascading event
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState;

    fn ev_transit_memory_state(
        current_state: Self::LoopState,
        memory_state_transition: Arc<MemoryStateTransition>,
        thread_arg: &Self::ThreadArg,

        // for cascading event
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState;

    /// Worker thread's entry point
    fn run(
        main_job_lock: Arc<MainJobLock>,
        event_queue: Arc<NonBlockingEventQueue>,
        stop_receiver: mpsc::Receiver<()>,
        worker_setup_coordinator: Arc<WorkerSetupCoordinator>,
        worker_stop_coordinator: Arc<WorkerStopCoordinator>,
        thread_arg: Self::ThreadArg,
    ) {
        let event_polls = Self::event_subscription()
            .into_iter()
            .map(|ev| event_queue.subscribe(ev))
            .collect();
        let _ = thread::Builder::new()
            .name(Self::THREAD_NAME.into())
            .spawn(move || {
                Self::main_loop(
                    main_job_lock,
                    event_queue,
                    event_polls,
                    stop_receiver,
                    worker_setup_coordinator,
                    worker_stop_coordinator,
                    thread_arg,
                )
            });
    }

    fn main_loop(
        main_job_lock: Arc<MainJobLock>,
        event_queue: Arc<NonBlockingEventQueue>,
        event_polls: Vec<EventPoll>,
        stop_receiver: mpsc::Receiver<()>,
        worker_setup_coordinator: Arc<WorkerSetupCoordinator>,
        worker_stop_coordinator: Arc<WorkerStopCoordinator>,
        thread_arg: Self::ThreadArg,
    ) {
        log::info!("[{}] main loop started", Self::THREAD_NAME);
        Self::setup_ready(worker_setup_coordinator.clone());
        worker_setup_coordinator.sync_wait_all_workers();

        let mut state = Self::LoopState::new(&thread_arg);

        while stop_receiver.try_recv().is_err() {
            if let Ok(_lock) = main_job_lock.try_main_job() {
                if state.is_integral() {
                    state = Self::main_loop_cycle(state, &thread_arg, event_queue.as_ref());
                }
            }
            state = Self::handle_events(state, &event_polls, &thread_arg, event_queue.clone());
        }

        log::info!(
            "[{}] main loop finished. Synchronize other threads to finish...",
            Self::THREAD_NAME
        );
        worker_stop_coordinator.sync_wait_all_workers();
    }

    fn setup_ready(worker_setup_coordinator: Arc<WorkerSetupCoordinator>);

    fn handle_events(
        current_state: Self::LoopState,
        event_polls: &[EventPoll],
        thread_arg: &Self::ThreadArg,
        nb_event_queue: Arc<NonBlockingEventQueue>, // to publish next events in event handlers
    ) -> Self::LoopState {
        let mut state = current_state;

        for event_poll in event_polls {
            #[allow(clippy::single_match)]
            while let Some(ev) = event_poll.poll() {
                match ev {
                    Event::UpdatePipeline {
                        pipeline_derivatives,
                    } => {
                        state = Self::ev_update_pipeline(
                            state,
                            pipeline_derivatives,
                            thread_arg,
                            nb_event_queue.clone(),
                        );
                    }
                    Event::ReplacePerformanceMetrics { metrics } => {
                        state = Self::ev_replace_performance_metrics(
                            state,
                            metrics,
                            thread_arg,
                            nb_event_queue.clone(),
                        );
                    }
                    Event::IncrementalUpdateMetrics {
                        metrics_update_by_task_execution_or_purge,
                    } => {
                        state = Self::ev_incremental_update_metrics(
                            state,
                            metrics_update_by_task_execution_or_purge,
                            thread_arg,
                            nb_event_queue.clone(),
                        )
                    }
                    Event::ReportMetricsSummary { metrics_summary } => {
                        state = Self::ev_report_metrics_summary(
                            state,
                            metrics_summary,
                            thread_arg,
                            nb_event_queue.clone(),
                        )
                    }
                    Event::TransitMemoryState {
                        memory_state_transition,
                    } => {
                        state = Self::ev_transit_memory_state(
                            state,
                            memory_state_transition,
                            thread_arg,
                            nb_event_queue.clone(),
                        )
                    }
                }
            }
        }
        state
    }
}
