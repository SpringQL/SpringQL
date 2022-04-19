// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use super::{
    main_job_lock::MainJobLock,
    task_executor::task_executor_lock::TaskExecutorLock,
    worker::worker_handle::{WorkerSetupCoordinator, WorkerStopCoordinator},
};

#[derive(Clone, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct Locks {
    pub(in crate::stream_engine::autonomous_executor) main_job_lock: Arc<MainJobLock>,
    pub(in crate::stream_engine::autonomous_executor) task_executor_lock: Arc<TaskExecutorLock>,
}

#[derive(Clone, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct Coordinators {
    pub(in crate::stream_engine::autonomous_executor) worker_setup_coordinator:
        Arc<WorkerSetupCoordinator>,
    pub(in crate::stream_engine::autonomous_executor) worker_stop_coordinator:
        Arc<WorkerStopCoordinator>,
}
