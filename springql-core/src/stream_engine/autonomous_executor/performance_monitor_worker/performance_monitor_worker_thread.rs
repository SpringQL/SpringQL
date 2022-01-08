use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crate::{
    pipeline::Pipeline,
    stream_engine::{
        autonomous_executor::{
            event_queue::{
                event::{Event, EventTag},
                EventPoll, EventQueue,
            },
            performance_metrics::PerformanceMetrics,
            pipeline_derivatives::PipelineDerivatives,
        },
        time::duration::wall_clock_duration::WallClockDuration,
    },
};

use super::web_console_reporter::WebConsoleReporter;

// TODO config
const CLOCK_MSEC: u64 = 100;
const REPORT_INTERVAL_CLOCK_WEB_CONSOLE: u64 = 30;
const WEB_CONSOLE_HOST: &str = "127.0.0.1";
const WEB_CONSOLE_PORT: u16 = 8050;
const WEB_CONSOLE_TIMEOUT: WallClockDuration =
    WallClockDuration::from_millis(CLOCK_MSEC * REPORT_INTERVAL_CLOCK_WEB_CONSOLE);

/// Runs a worker thread.
#[derive(Debug)]
pub(super) struct PerformanceMonitorWorkerThread;

impl PerformanceMonitorWorkerThread {
    pub(super) fn run(event_queue: Arc<EventQueue>, stop_receiver: mpsc::Receiver<()>) {
        let event_poll_update_pipeline = event_queue.subscribe(EventTag::UpdatePipeline);

        let web_console_reporter =
            WebConsoleReporter::new(WEB_CONSOLE_HOST, WEB_CONSOLE_PORT, WEB_CONSOLE_TIMEOUT);

        let _ = thread::spawn(move || {
            Self::main_loop(
                web_console_reporter,
                event_poll_update_pipeline,
                stop_receiver,
            )
        });
    }

    fn main_loop(
        web_console_reporter: WebConsoleReporter,
        event_poll_update_pipeline: EventPoll,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        let mut pipeline_derivatives = Arc::new(PipelineDerivatives::new(Pipeline::default()));
        let mut metrics = PerformanceMetrics::default();
        let mut clk_wc = REPORT_INTERVAL_CLOCK_WEB_CONSOLE;

        log::debug!("[PerformanceMonitorWorker] Started");

        while stop_receiver.try_recv().is_err() {
            pipeline_derivatives = Self::handle_interruption(
                &event_poll_update_pipeline,
                pipeline_derivatives,
                &mut metrics,
            );

            if clk_wc == 0 {
                clk_wc = REPORT_INTERVAL_CLOCK_WEB_CONSOLE;
                web_console_reporter.report(&metrics, pipeline_derivatives.task_graph());
            }

            thread::sleep(Duration::from_millis(CLOCK_MSEC))
        }
    }

    /// May re-create PipelineDerivatives and PerformanceMetrics.
    ///
    /// # Returns
    ///
    /// Some on interruption.
    fn handle_interruption(
        event_poll_update_pipeline: &EventPoll,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        metrics: &mut PerformanceMetrics,
    ) -> Arc<PipelineDerivatives> {
        if let Some(Event::UpdatePipeline {
            pipeline_derivatives,
        }) = event_poll_update_pipeline.poll()
        {
            log::debug!("[PerformanceMonitorWorker] got UpdatePipeline event");
            metrics.reset_from_task_graph(pipeline_derivatives.task_graph());
            pipeline_derivatives
        } else {
            pipeline_derivatives
        }
    }
}
