// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod worker;

use std::sync::Arc;

use crate::stream_engine::{
    autonomous_executor::{
        current_pipeline::CurrentPipeline,
        task::{
            sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
            source_task::source_reader::source_reader_repository::SourceReaderRepository,
        },
    },
    dependency_injection::DependencyInjection,
};

use self::worker::{worker_id::WorkerId, Worker};

use super::scheduler::scheduler_read::SchedulerRead;

#[derive(Debug)]
pub(super) struct WorkerPool(Vec<Worker>);

impl WorkerPool {
    pub(super) fn new<DI: DependencyInjection>(
        n_worker_threads: usize,
        current_pipeline: Arc<CurrentPipeline>,
        scheduler_read: SchedulerRead<DI>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        let workers = (0..n_worker_threads)
            .map(|id| {
                Worker::new::<DI>(
                    WorkerId::new(id as u16),
                    current_pipeline.clone(),
                    scheduler_read.clone(),
                    row_repo.clone(),
                    source_reader_repo.clone(),
                    sink_writer_repo.clone(),
                )
            })
            .collect();
        Self(workers)
    }
}
