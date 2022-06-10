// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    event_queue::{BlockingEventQueue, NonBlockingEventQueue},
    main_job_lock::MainJobLock,
    task_executor::TaskExecutorLock,
    worker::{WorkerSetupCoordinator, WorkerStopCoordinator},
};

#[derive(Clone, Debug, new)]
pub struct Locks {
    pub main_job_lock: Arc<MainJobLock>,
    pub task_executor_lock: Arc<TaskExecutorLock>,
}

#[derive(Clone, Debug, new)]
pub struct EventQueues {
    pub blocking: Arc<BlockingEventQueue>,
    pub non_blocking: Arc<NonBlockingEventQueue>,
}

#[derive(Clone, Debug, new)]
pub struct Coordinators {
    pub worker_setup_coordinator: Arc<WorkerSetupCoordinator>,
    pub worker_stop_coordinator: Arc<WorkerStopCoordinator>,
}
