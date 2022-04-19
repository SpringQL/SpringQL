// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod performance_monitor_worker_thread;

mod web_console_reporter;

use std::sync::Arc;

use crate::low_level_rs::SpringConfig;

use self::performance_monitor_worker_thread::{
    PerformanceMonitorWorkerThread, PerformanceMonitorWorkerThreadArg,
};

use super::{
    event_queue::{
        blocking_event_queue::BlockingEventQueue, non_blocking_event_queue::NonBlockingEventQueue,
    },
    main_job_lock::MainJobLock,
    worker::worker_handle::{WorkerHandle, WorkerSetupCoordinator, WorkerStopCoordinator},
};

/// Dedicated thread to:
///
/// 1. Monitor performance of task graphs via [PerformanceMetrics](crate::stream_engine::autonomous_executor::performance_monitor::PerformanceMetrics).
/// 2. Report the performance to [AutonomousExecutor](crate::stream_processor::autonomous_executor::AutonomousExecutor) and web-console.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct PerformanceMonitorWorker {
    _handle: WorkerHandle,
}

impl PerformanceMonitorWorker {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        config: &SpringConfig,
        main_job_lock: Arc<MainJobLock>,
        b_event_queue: Arc<BlockingEventQueue>,
        nb_event_queue: Arc<NonBlockingEventQueue>,
        worker_setup_coordinator: Arc<WorkerSetupCoordinator>,
        worker_stop_coordinator: Arc<WorkerStopCoordinator>,
    ) -> Self {
        let handle = WorkerHandle::new::<PerformanceMonitorWorkerThread>(
            main_job_lock,
            b_event_queue,
            nb_event_queue,
            worker_setup_coordinator,
            worker_stop_coordinator,
            PerformanceMonitorWorkerThreadArg::from(config),
        );
        Self { _handle: handle }
    }
}
