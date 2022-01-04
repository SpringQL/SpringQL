// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::{
    autonomous_executor::current_pipeline::CurrentPipeline,
    dependency_injection::DependencyInjection,
};

use super::{
    sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
    source_task::source_reader::source_reader_repository::SourceReaderRepository, task_id::TaskId,
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskContext<DI: DependencyInjection> {
    task: TaskId,

    // why a task need to know pipeline? -> source tasks need to know source stream's shape.
    current_pipeline: Arc<CurrentPipeline>,

    row_repo: Arc<DI::RowRepositoryType>,

    source_reader_repo: Arc<SourceReaderRepository>,
    sink_writer_repo: Arc<SinkWriterRepository>,
}

impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        task: TaskId,
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        Self {
            task,
            current_pipeline,
            row_repo,
            source_reader_repo,
            sink_writer_repo,
        }
    }

    pub(in crate::stream_engine) fn task(&self) -> TaskId {
        self.task.clone()
    }

    pub(in crate::stream_engine) fn current_pipeline(&self) -> Arc<CurrentPipeline> {
        self.current_pipeline.clone()
    }

    pub(in crate::stream_engine) fn downstream_tasks(&self) -> Vec<TaskId> {
        let task_graph = self.current_pipeline.task_graph();
        task_graph.downstream_tasks(self.task.clone())
    }

    pub(in crate::stream_engine) fn row_repository(&self) -> Arc<DI::RowRepositoryType> {
        self.row_repo.clone()
    }

    pub(in crate::stream_engine) fn source_reader_repository(&self) -> Arc<SourceReaderRepository> {
        self.source_reader_repo.clone()
    }
    pub(in crate::stream_engine) fn sink_writer_repository(&self) -> Arc<SinkWriterRepository> {
        self.sink_writer_repo.clone()
    }
}

#[cfg(test)]
impl<DI: DependencyInjection> TaskContext<DI> {
    pub(in crate::stream_engine) fn _test_factory(
        task: TaskId,
        current_pipeline: Arc<CurrentPipeline>,
        row_repo: Arc<DI::RowRepositoryType>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        Self {
            task,
            current_pipeline,
            row_repo,
            source_reader_repo,
            sink_writer_repo,
        }
    }
}
