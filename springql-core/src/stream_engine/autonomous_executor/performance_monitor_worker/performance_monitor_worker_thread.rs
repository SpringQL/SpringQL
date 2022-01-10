use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::{
    autonomous_executor::{
        event_queue::{
            event::{Event, EventTag},
            EventQueue,
        },
        performance_metrics::{
            metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
            PerformanceMetrics,
        },
        pipeline_derivatives::PipelineDerivatives,
        worker::worker_thread::{WorkerThread, WorkerThreadLoopState},
    },
    time::duration::wall_clock_duration::WallClockDuration,
};

use super::web_console_reporter::WebConsoleReporter;

// TODO config
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
    web_console_reporter: WebConsoleReporter,
}

impl Default for PerformanceMonitorWorkerThreadArg {
    fn default() -> Self {
        // TODO ::from_config()
        let web_console_reporter =
            WebConsoleReporter::new(WEB_CONSOLE_HOST, WEB_CONSOLE_PORT, WEB_CONSOLE_TIMEOUT);
        Self {
            web_console_reporter,
        }
    }
}

#[derive(Debug)]
pub(super) struct PerformanceMonitorWorkerLoopState {
    pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
    metrics: Option<Arc<PerformanceMetrics>>,
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
        _event_queue: &EventQueue,
    ) -> Self::LoopState {
        let mut state = current_state;
        if let (Some(pipeline_derivatives), Some(metrics)) =
            (&state.pipeline_derivatives, &state.metrics)
        {
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
}
