// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod performance_monitor_worker_thread;

mod web_console_reporter;

pub use performance_monitor_worker_thread::PerformanceMonitorWorkerThread;

use std::sync::Arc;

use crate::{
    api::SpringConfig,
    stream_engine::autonomous_executor::{
        args::{Coordinators, EventQueues},
        main_job_lock::MainJobLock,
        performance_monitor_worker::performance_monitor_worker_thread::PerformanceMonitorWorkerThreadArg,
        worker::WorkerHandle,
    },
};

/// Dedicated thread to:
///
/// 1. Monitor performance of task graphs via `PerformanceMetrics`.
/// 2. Report the performance to `AutonomousExecutor` and web-console.
#[derive(Debug)]
pub struct PerformanceMonitorWorker {
    _handle: WorkerHandle,
}

impl PerformanceMonitorWorker {
    pub fn new(
        config: &SpringConfig,
        main_job_lock: Arc<MainJobLock>,
        event_queues: EventQueues,
        coordinators: Coordinators,
    ) -> Self {
        let handle = WorkerHandle::new::<PerformanceMonitorWorkerThread>(
            main_job_lock,
            event_queues,
            coordinators,
            PerformanceMonitorWorkerThreadArg::from(config),
        );
        Self { _handle: handle }
    }
}
