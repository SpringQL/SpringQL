// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod performance_monitor_worker_thread;

mod web_console_reporter;

use std::sync::Arc;

use self::performance_monitor_worker_thread::{
    PerformanceMonitorWorkerThread, PerformanceMonitorWorkerThreadArg,
};

use super::{event_queue::EventQueue, worker::worker_handle::WorkerHandle};

/// Dedicated thread to:
///
/// 1. Monitor performance of task graphs via [PerformanceMetrics](crate::stream_engine::autonomous_executor::performance_monitor::PerformanceMetrics).
/// 2. Report the performance to [AutonomousExecutor](crate::stream_processor::autonomous_executor::AutonomousExecutor) and web-console.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMonitorWorker {
    handle: WorkerHandle,
}

impl PerformanceMonitorWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(event_queue: Arc<EventQueue>) -> Self {
        let handle = WorkerHandle::new::<PerformanceMonitorWorkerThread>(
            event_queue,
            PerformanceMonitorWorkerThreadArg::default(),
        );
        Self { handle }
    }
}
