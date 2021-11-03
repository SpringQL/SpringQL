pub(super) mod worker;

use std::sync::Arc;

use crate::stream_engine::dependency_injection::DependencyInjection;

use self::worker::{worker_id::WorkerId, Worker};

use super::{
    scheduler::scheduler_read::SchedulerRead, server::server_repository::ServerRepository,
};

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        scheduler_read: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        server_repo: Arc<ServerRepository>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                Worker::new::<DI>(
                    WorkerId::new(id as u16),
                    scheduler_read.clone(),
                    row_repo.clone(),
                    server_repo.clone(),
                )
            })
            .collect();
        Self(workers)
    }
}
