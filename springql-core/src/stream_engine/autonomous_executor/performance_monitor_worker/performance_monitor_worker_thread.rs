// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{sync::Arc, thread, time::Duration};

use time::OffsetDateTime;

use crate::{
    api::SpringConfig,
    stream_engine::{
        autonomous_executor::{
            event_queue::{
                BlockingEventTag, Event, EventTag, NonBlockingEventQueue, NonBlockingEventTag,
            },
            memory_state_machine::MemoryStateTransition,
            performance_metrics::{
                MetricsUpdateByTaskExecutionOrPurge, PerformanceMetrics, PerformanceMetricsSummary,
            },
            performance_monitor_worker::web_console_reporter::WebConsoleReporter,
            pipeline_derivatives::PipelineDerivatives,
            worker::{WorkerSetupCoordinator, WorkerThread, WorkerThreadLoopState},
        },
        time::{SpringDuration, WallClockDuration},
    },
};

const CLOCK_MSEC: u64 = 10;

/// Runs a worker thread.
#[derive(Debug)]
pub struct PerformanceMonitorWorkerThread;

#[derive(Debug)]
pub struct PerformanceMonitorWorkerThreadArg {
    config: SpringConfig,
    web_console_reporter: Option<WebConsoleReporter>,
}

impl From<&SpringConfig> for PerformanceMonitorWorkerThreadArg {
    fn from(config: &SpringConfig) -> Self {
        let web_console_reporter = if config.web_console.enable_report_post {
            Some(WebConsoleReporter::new(
                &config.web_console.host,
                config.web_console.port,
                WallClockDuration::from_millis(config.web_console.timeout_msec as u64),
            ))
        } else {
            None
        };
        Self {
            config: config.clone(),
            web_console_reporter,
        }
    }
}

#[derive(Debug)]
pub struct PerformanceMonitorWorkerLoopState {
    pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
    metrics: Option<Arc<PerformanceMetrics>>,
    countdown_metrics_summary_msec: i32,
    countdown_web_console_msec: i32,
}

impl WorkerThreadLoopState for PerformanceMonitorWorkerLoopState {
    type ThreadArg = PerformanceMonitorWorkerThreadArg;

    fn new(thread_arg: &Self::ThreadArg) -> Self
    where
        Self: Sized,
    {
        Self {
            pipeline_derivatives: None,
            metrics: None,
            countdown_metrics_summary_msec: thread_arg
                .config
                .memory
                .performance_metrics_summary_report_interval_msec
                as i32,
            countdown_web_console_msec: thread_arg.config.web_console.report_interval_msec as i32,
        }
    }

    fn is_integral(&self) -> bool {
        match (&self.pipeline_derivatives, &self.metrics) {
            (Some(p), Some(m)) => p.pipeline_version() == *m.pipeline_version(),
            _ => false,
        }
    }
}

impl WorkerThread for PerformanceMonitorWorkerThread {
    const THREAD_NAME: &'static str = "PerformanceMonitorWorker";

    type ThreadArg = PerformanceMonitorWorkerThreadArg;

    type LoopState = PerformanceMonitorWorkerLoopState;

    fn setup_ready(worker_setup_coordinator: Arc<WorkerSetupCoordinator>) {
        worker_setup_coordinator.ready_performance_monitor_worker_worker()
    }

    fn event_subscription() -> Vec<EventTag> {
        vec![
            EventTag::Blocking(BlockingEventTag::UpdatePipeline),
            EventTag::NonBlocking(NonBlockingEventTag::IncrementalUpdateMetrics),
        ]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
        event_queue: &NonBlockingEventQueue,
    ) -> Self::LoopState {
        if let (Some(pipeline_derivatives), Some(metrics)) = (
            current_state.pipeline_derivatives.clone(),
            current_state.metrics.clone(),
        ) {
            let mut state = current_state;

            state = Self::report_metrics_summary(
                state,
                metrics.as_ref(),
                event_queue,
                thread_arg
                    .config
                    .memory
                    .performance_metrics_summary_report_interval_msec as i32,
            );

            if let Some(web_console_reporter) = &thread_arg.web_console_reporter {
                state = Self::post_web_console(
                    state,
                    pipeline_derivatives.as_ref(),
                    metrics.as_ref(),
                    web_console_reporter,
                    thread_arg.config.web_console.report_interval_msec as i32,
                );
            }

            thread::sleep(Duration::from_millis(CLOCK_MSEC));

            state
        } else {
            unreachable!("by integrity check")
        }
    }

    fn ev_update_pipeline(
        current_state: Self::LoopState,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        _thread_arg: &Self::ThreadArg,
        event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        let mut state = current_state;

        let metrics = Arc::new(PerformanceMetrics::from_task_graph(
            pipeline_derivatives.task_graph(),
        ));
        state.metrics = Some(metrics.clone());
        event_queue.publish(Event::ReplacePerformanceMetrics { metrics });

        state.pipeline_derivatives = Some(pipeline_derivatives);

        state
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
        current_state: Self::LoopState,
        metrics: Arc<MetricsUpdateByTaskExecutionOrPurge>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        let state = current_state;
        if let Some(m) = state.metrics.as_ref() {
            match metrics.as_ref() {
                MetricsUpdateByTaskExecutionOrPurge::TaskExecution(metrics_diff) => {
                    m.update_by_task_execution(metrics_diff)
                }
                MetricsUpdateByTaskExecutionOrPurge::Purge => m.update_by_purge(),
            }
        }
        state
    }

    fn ev_report_metrics_summary(
        _current_state: Self::LoopState,
        _metrics_summary: Arc<PerformanceMetricsSummary>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<NonBlockingEventQueue>,
    ) -> Self::LoopState {
        unreachable!()
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

impl PerformanceMonitorWorkerThread {
    fn report_metrics_summary(
        state: PerformanceMonitorWorkerLoopState,
        metrics: &PerformanceMetrics,
        event_queue: &NonBlockingEventQueue,
        report_interval_msec: i32,
    ) -> PerformanceMonitorWorkerLoopState {
        let mut state = state;

        if state.countdown_metrics_summary_msec <= 0 {
            state.countdown_metrics_summary_msec = report_interval_msec;

            let metrics_summary = Arc::new(PerformanceMetricsSummary::from(metrics));
            let now = OffsetDateTime::now_utc();
            println!(
                "GRAPH: PerformanceMonitorWorkerThread::report_metrics_summary: ({}, {})",
                now.unix_timestamp() as f64 + now.unix_timestamp_nanos() as f64 / 1_000_000_000.0,
                metrics_summary.queue_total_bytes
            );
            event_queue.publish(Event::ReportMetricsSummary { metrics_summary })
        } else {
            state.countdown_metrics_summary_msec -= CLOCK_MSEC as i32;
        }

        state
    }

    fn post_web_console(
        state: PerformanceMonitorWorkerLoopState,
        pipeline_derivatives: &PipelineDerivatives,
        metrics: &PerformanceMetrics,
        web_console_reporter: &WebConsoleReporter,
        report_interval_msec: i32,
    ) -> PerformanceMonitorWorkerLoopState {
        let mut state = state;

        if state.countdown_web_console_msec <= 0 {
            state.countdown_web_console_msec = report_interval_msec as i32;

            web_console_reporter.report(metrics, pipeline_derivatives.task_graph());
        } else {
            state.countdown_web_console_msec -= CLOCK_MSEC as i32;
        }

        state
    }
}
