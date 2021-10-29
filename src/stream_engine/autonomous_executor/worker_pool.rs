mod worker;

use std::sync::{Arc, Mutex};

use self::worker::Worker;

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new(n_worker_threads: usize, scheduler: Arc<Mutex<Scheduler>>) -> Self {
        let workers = (0..n_worker_threads)
            .map(|_| Worker::new(scheduler.clone()))
            .collect();
        Self(workers)
    }
}
