// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod worker;

use std::sync::Arc;

use crate::stream_engine::dependency_injection::DependencyInjection;

use self::worker::{worker_id::WorkerId, Worker};

use super::{
    scheduler::scheduler_read::SchedulerRead, server_instance::server_repository::ServerRepository,
    task::source_task::source_subtask::source_subtask_repository::SourceSubtaskRepository,
};

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        scheduler_read: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_subtask_repo: Arc<SourceSubtaskRepository>,
        server_repo: Arc<ServerRepository>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                Worker::new::<DI>(
                    WorkerId::new(id as u16),
                    scheduler_read.clone(),
                    row_repo.clone(),
                    source_subtask_repo.clone(),
                    server_repo.clone(),
                )
            })
            .collect();
        Self(workers)
    }
}
