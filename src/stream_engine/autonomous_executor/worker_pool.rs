mod worker;

use std::sync::{Arc, Mutex};

use crate::stream_engine::dependency_injection::DependencyInjection;

use self::worker::Worker;

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        scheduler: Arc<Mutex<DI::SchedulerType>>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|_| Worker::new::<DI>(scheduler.clone()))
            .collect();
        Self(workers)
    }
}
