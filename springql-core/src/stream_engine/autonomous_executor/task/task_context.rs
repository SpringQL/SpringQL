// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    pipeline_derivatives::PipelineDerivatives, row::row_repository::RowRepository,
    task_graph::task_id::TaskId,
};

use super::{
    sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
    source_task::source_reader::source_reader_repository::SourceReaderRepository,
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct TaskContext {
    task: TaskId,

    // why a task need to know pipeline? -> source tasks need to know source stream's shape.
    pipeline_derivatives: Arc<PipelineDerivatives>,

    row_repo: Arc<RowRepository>,

    source_reader_repo: Arc<SourceReaderRepository>,
    sink_writer_repo: Arc<SinkWriterRepository>,
}

impl TaskContext {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        task: TaskId,
        pipeline_derivatives: Arc<PipelineDerivatives>,
        row_repo: Arc<RowRepository>,
        source_reader_repo: Arc<SourceReaderRepository>,
        sink_writer_repo: Arc<SinkWriterRepository>,
    ) -> Self {
        Self {
            task,
            pipeline_derivatives,
            row_repo,
            source_reader_repo,
            sink_writer_repo,
        }
    }

    pub(in crate::stream_engine) fn task(&self) -> TaskId {
        self.task.clone()
    }

    pub(in crate::stream_engine) fn pipeline_derivatives(&self) -> Arc<PipelineDerivatives> {
        self.pipeline_derivatives.clone()
    }

    pub(in crate::stream_engine) fn downstream_tasks(&self) -> Vec<TaskId> {
        let task_graph = self.pipeline_derivatives.task_graph();
        task_graph.downstream_tasks(&self.task)
    }

    pub(in crate::stream_engine) fn row_repository(&self) -> Arc<RowRepository> {
        self.row_repo.clone()
    }

    pub(in crate::stream_engine) fn source_reader_repository(&self) -> Arc<SourceReaderRepository> {
        self.source_reader_repo.clone()
    }
    pub(in crate::stream_engine) fn sink_writer_repository(&self) -> Arc<SinkWriterRepository> {
        self.sink_writer_repo.clone()
    }
}
