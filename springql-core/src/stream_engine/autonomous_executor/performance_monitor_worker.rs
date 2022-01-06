// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod autonomous_executor_reporter;
mod web_console_reporter;

// TODO config
const CLOCK_MSEC: u64 = 100;
const REPORT_INTERVAL_CLOCK_AUTONOMOUS_EXECUTOR: u64 = 5;
const REPORT_INTERVAL_CLOCK_WEB_CONSOLE: u64 = 30;
const WEB_CONSOLE_HOST: &str = "127.0.0.1";
const WEB_CONSOLE_PORT: u16 = 8050;
const WEB_CONSOLE_TIMEOUT: WallClockDuration =
    WallClockDuration::from_millis(CLOCK_MSEC * REPORT_INTERVAL_CLOCK_AUTONOMOUS_EXECUTOR);

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use crate::stream_engine::{
    autonomous_executor::{
        performance_metrics::{
            performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
        },
        task_graph::TaskGraph,
    },
    time::duration::wall_clock_duration::WallClockDuration,
};

use self::{
    autonomous_executor_reporter::AutonomousExecutorReporter,
    web_console_reporter::WebConsoleReporter,
};

/// Dedicated thread to:
///
/// 1. Monitor performance of task graphs via [PerformanceMetrics](crate::stream_engine::autonomous_executor::performance_monitor::PerformanceMetrics).
/// 2. Report the performance to [AutonomousExecutor](crate::stream_processor::autonomous_executor::AutonomousExecutor) and web-console.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMonitorWorker {
    self_stop_button: mpsc::SyncSender<()>,
}

impl PerformanceMonitorWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        metrics: Arc<PerformanceMetrics>,
        graph: Arc<TaskGraph>,
        autonomous_executor: mpsc::Sender<PerformanceMetricsSummary>,
    ) -> Self {
        let autonomous_executor_reporter = AutonomousExecutorReporter::new(autonomous_executor);
        let web_console_reporter =
            WebConsoleReporter::new(WEB_CONSOLE_HOST, WEB_CONSOLE_PORT, WEB_CONSOLE_TIMEOUT);

        let (self_stop_button, stop_receiver) = mpsc::sync_channel(0);

        let _ = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                Self::main_loop(
                    metrics.clone(),
                    graph.clone(),
                    autonomous_executor_reporter,
                    web_console_reporter,
                    stop_receiver,
                )
                .await
            });
        });
        Self { self_stop_button }
    }

    async fn main_loop(
        metrics: Arc<PerformanceMetrics>,
        graph: Arc<TaskGraph>,
        autonomous_executor_reporter: AutonomousExecutorReporter,
        web_console_reporter: WebConsoleReporter,
        stop_receiver: mpsc::Receiver<()>,
    ) {
        log::debug!("[PeformanceMonitorWorker] Started");

        let mut clk_ae = REPORT_INTERVAL_CLOCK_AUTONOMOUS_EXECUTOR;
        let mut clk_wc = REPORT_INTERVAL_CLOCK_WEB_CONSOLE;

        while stop_receiver.try_recv().is_err() {
            if clk_ae == 0 {
                clk_ae = REPORT_INTERVAL_CLOCK_AUTONOMOUS_EXECUTOR;
                autonomous_executor_reporter.report(metrics.as_ref());
            }
            if clk_wc == 0 {
                clk_wc = REPORT_INTERVAL_CLOCK_WEB_CONSOLE;
                web_console_reporter
                    .report(metrics.as_ref(), graph.as_ref())
                    .await;
            }
            thread::sleep(Duration::from_millis(CLOCK_MSEC))
        }
    }
}

impl Drop for PerformanceMonitorWorker {
    fn drop(&mut self) {
        self.self_stop_button
            .send(())
            .expect("failed to wait for PerformanceMonitorWorker thread to finish its job");

        log::debug!("[PeformanceMonitorWorker] Stopped");
    }
}
