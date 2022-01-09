use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::{
    autonomous_executor::{
        event_queue::{
            event::{Event, EventTag},
            EventQueue,
        },
        performance_metrics::PerformanceMetrics,
        pipeline_derivatives::PipelineDerivatives,
        worker::worker_thread::WorkerThread,
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

#[derive(Debug, Default)]
pub(super) struct PerformanceMonitorWorkerLoopState {
    metrics: Arc<PerformanceMetrics>,
    pipeline_derivatives: Arc<PipelineDerivatives>,
    clk_web_console: u64,
}

impl WorkerThread for PerformanceMonitorWorkerThread {
    type ThreadArg = PerformanceMonitorWorkerThreadArg;

    type LoopState = PerformanceMonitorWorkerLoopState;

    fn event_subscription() -> Vec<EventTag> {
        vec![EventTag::UpdatePipeline]
    }

    fn main_loop_cycle(
        current_state: Self::LoopState,
        thread_arg: &Self::ThreadArg,
        event_queue: &EventQueue,
    ) -> Self::LoopState {
        let mut state = current_state;

        if state.clk_web_console == 0 {
            state.clk_web_console = WEB_CONSOLE_REPORT_INTERVAL_CLOCK;
            thread_arg
                .web_console_reporter
                .report(&state.metrics, state.pipeline_derivatives.task_graph());
        }
        thread::sleep(Duration::from_millis(CLOCK_MSEC));

        state
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
        state.metrics = metrics.clone();
        event_queue.publish(Event::UpdatePerformanceMetrics { metrics });

        state.pipeline_derivatives = pipeline_derivatives;

        state
    }

    fn ev_update_performance_metrics(
        _current_state: Self::LoopState,
        _metrics: Arc<PerformanceMetrics>,
        _thread_arg: &Self::ThreadArg,
        _event_queue: Arc<EventQueue>,
    ) -> Self::LoopState {
        unreachable!()
    }
}
