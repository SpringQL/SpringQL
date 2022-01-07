// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod performance_monitor_worker_thread;
mod web_console_reporter;

use std::sync::{mpsc, Arc};

use self::performance_monitor_worker_thread::PerformanceMonitorWorkerThread;

use super::{event_queue::EventQueue, performance_metrics::PerformanceMetrics};

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

        let _ = PerformanceMonitorWorkerThread::run(event_queue, stop_receiver);
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
