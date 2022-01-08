// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod performance_monitor_worker_thread;
mod web_console_reporter;

use std::sync::{mpsc, Arc};

use crate::stream_engine::time::duration::wall_clock_duration::WallClockDuration;

use self::{
    performance_monitor_worker_thread::{
        PerformanceMonitorWorkerThread, PerformanceMonitorWorkerThreadArg,
    },
    web_console_reporter::WebConsoleReporter,
};

use super::{event_queue::EventQueue, worker::worker_thread::WorkerThread};

// TODO config
const CLOCK_MSEC: u64 = 100;
const REPORT_INTERVAL_CLOCK_WEB_CONSOLE: u64 = 30;
const WEB_CONSOLE_HOST: &str = "127.0.0.1";
const WEB_CONSOLE_PORT: u16 = 8050;
const WEB_CONSOLE_TIMEOUT: WallClockDuration =
    WallClockDuration::from_millis(CLOCK_MSEC * REPORT_INTERVAL_CLOCK_WEB_CONSOLE);

/// Dedicated thread to:
///
/// 1. Monitor performance of task graphs via [PerformanceMetrics](crate::stream_engine::autonomous_executor::performance_monitor::PerformanceMetrics).
/// 2. Report the performance to [AutonomousExecutor](crate::stream_processor::autonomous_executor::AutonomousExecutor) and web-console.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMonitorWorker {
    stop_button: mpsc::SyncSender<()>,
}

impl PerformanceMonitorWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(event_queue: Arc<EventQueue>) -> Self {
        let (stop_button, stop_receiver) = mpsc::sync_channel(0);

        let arg = PerformanceMonitorWorkerThreadArg::new(WebConsoleReporter::new(
            WEB_CONSOLE_HOST,
            WEB_CONSOLE_PORT,
            WEB_CONSOLE_TIMEOUT,
        ));
        let _ = PerformanceMonitorWorkerThread::run(event_queue, stop_receiver, arg);
        Self { stop_button }
    }
}

impl Drop for PerformanceMonitorWorker {
    fn drop(&mut self) {
        self.stop_button
            .send(())
            .expect("failed to wait for PerformanceMonitorWorker thread to finish its job");

        log::debug!("[PeformanceMonitorWorker] Stopped");
    }
}
