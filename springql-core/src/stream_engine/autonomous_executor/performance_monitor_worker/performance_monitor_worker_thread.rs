use std::{sync::Arc, thread, time::Duration};

use crate::{
    low_level_rs::{SpringConfig, SpringMemoryConfig},
    stream_engine::{
        autonomous_executor::{
            event_queue::{
                event::{Event, EventTag},
                EventQueue,
            },
            memory_state_machine::MemoryStateTransition,
            performance_metrics::{
                metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
                performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
            },
            pipeline_derivatives::PipelineDerivatives,
            worker::worker_thread::{WorkerThread, WorkerThreadLoopState},
        },
        time::duration::wall_clock_duration::WallClockDuration,
    },
};

use super::web_console_reporter::WebConsoleReporter;

const CLOCK_MSEC: u64 = 100;
const WEB_CONSOLE_REPORT_INTERVAL_CLOCK: u64 = 30;
const WEB_CONSOLE_HOST: &str = "127.0.0.1";
const WEB_CONSOLE_PORT: u16 = 8050;
const WEB_CONSOLE_TIMEOUT: WallClockDuration =
    WallClockDuration::from_millis(CLOCK_MSEC * WEB_CONSOLE_REPORT_INTERVAL_CLOCK);

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct PerformanceMonitorWorkerThread;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMonitorWorkerThreadArg {
    memory_config: SpringMemoryConfig,
    web_console_reporter: WebConsoleReporter,
}

impl From<&SpringConfig> for PerformanceMonitorWorkerThreadArg {
    fn from(config: &SpringConfig) -> Self {
        // TODO ::from_config()
        let web_console_reporter =
            WebConsoleReporter::new(WEB_CONSOLE_HOST, WEB_CONSOLE_PORT, WEB_CONSOLE_TIMEOUT);
        Self {
            memory_config: config.memory,
            web_console_reporter,
        }
    }
}

#[derive(Debug)]
pub(super) struct PerformanceMonitorWorkerLoopState {
    pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
    metrics: Option<Arc<PerformanceMetrics>>,
    countdown_metrics_summary_msec: i32,
    clk_web_console: u64,
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
                .memory_config
                .performance_metrics_summary_report_interval_msec
                as i32,
            clk_web_console: WEB_CONSOLE_REPORT_INTERVAL_CLOCK,
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
    type ThreadArg = PerformanceMonitorWorkerThreadArg;

    type LoopState = PerformanceMonitorWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline, EventTag::IncrementalUpdateMetrics]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
        event_queue: &EventQueue,
    ) -> Self::LoopState {
        let mut state = current_state;
        if let (Some(pipeline_derivatives), Some(metrics)) =
            (&state.pipeline_derivatives, &state.metrics)
        {
            if state.countdown_metrics_summary_msec <= 0 {
                // reset count
                state.countdown_metrics_summary_msec = thread_arg
                    .memory_config
                    .performance_metrics_summary_report_interval_msec
                    as i32;

                let metrics_summary = Arc::new(PerformanceMetricsSummary::from(metrics.as_ref()));
                event_queue.publish(Event::ReportMetricsSummary { metrics_summary })
            } else {
                state.countdown_metrics_summary_msec -= CLOCK_MSEC as i32;
            }

            if state.clk_web_console == 0 {
                state.clk_web_console = WEB_CONSOLE_REPORT_INTERVAL_CLOCK;
                thread_arg
                    .web_console_reporter
                    .report(metrics, pipeline_derivatives.task_graph());
            } else {
                state.clk_web_console -= 1;
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
        event_queue: Arc<EventQueue>,
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
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_incremental_update_metrics(
        current_state: Self::LoopState,
        metrics: Arc<MetricsUpdateByTaskExecution>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        let state = current_state;
        if let Some(m) = state.metrics.as_ref() {
            m.update_by_task_execution(metrics.as_ref())
        }
        state
    }

    fn ev_report_metrics_summary(
        _current_state: Self::LoopState,
        _metrics_summary: Arc<PerformanceMetricsSummary>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }

    fn ev_transit_memory_state(
        _current_state: Self::LoopState,
        _memory_state_transition: Arc<MemoryStateTransition>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }
}
