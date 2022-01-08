use std::{sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::event::EventTag, performance_metrics::PerformanceMetrics,
    pipeline_derivatives::PipelineDerivatives, worker::WorkerThread,
};

use super::{
    web_console_reporter::WebConsoleReporter, CLOCK_MSEC, REPORT_INTERVAL_CLOCK_WEB_CONSOLE,
};

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct PerformanceMonitorWorkerThread;

#[derive(Debug, new)]
pub(super) struct PerformanceMonitorWorkerThreadArg {
    web_console_reporter: WebConsoleReporter,
}

#[derive(Debug, Default)]
pub(super) struct PerformanceMonitorWorkerLoopState {
    pipeline_derivatives: Arc<PipelineDerivatives>,
    metrics: PerformanceMetrics,
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
    ) -> Self::LoopState {
        let mut state = current_state;

        if state.clk_web_console == 0 {
            state.clk_web_console = REPORT_INTERVAL_CLOCK_WEB_CONSOLE;
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
    ) -> Self::LoopState {
        let mut state = current_state;

        state
            .metrics
            .reset_from_task_graph(pipeline_derivatives.task_graph());
        state.pipeline_derivatives = pipeline_derivatives;

        state
    }
}
