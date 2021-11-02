pub(super) mod worker;

use std::sync::{Arc, Mutex};

use crate::stream_engine::dependency_injection::DependencyInjection;

use self::worker::Worker;

use super::scheduler::scheduler_read::SchedulerRead;

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        scheduler_read: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|i| Worker::new::<DI>(scheduler_read.clone(), row_repo.clone()))
            .collect();
        Self(workers)
    }
}
