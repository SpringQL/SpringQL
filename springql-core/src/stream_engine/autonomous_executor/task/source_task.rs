// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod source_reader;

use std::fmt::Debug;

use crate::error::Result;
use crate::pipeline::name::{SourceReaderName, StreamName};
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::stream_engine::autonomous_executor::row::Row;

use super::task_context::TaskContext;
use super::task_id::TaskId;

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    source_reader_name: SourceReaderName,
    source_stream_name: StreamName,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(source_reader: &SourceReaderModel) -> Self {
        let id = TaskId::from_source_reader(source_reader.dest_source_stream().clone());
        Self {
            id,
            source_reader_name: source_reader.name().clone(),
            source_stream_name: source_reader.dest_source_stream().clone(),
        }
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<()> {
        let row = self.collect_next(context)?;
        context
            .row_repository()
            .emit(row, &context.downstream_tasks())
    }

    fn collect_next(&self, context: &TaskContext) -> Result<Row> {
        let source_reader = context
            .source_reader_repository()
            .get_source_reader(&self.source_reader_name);

        let source_stream = context
            .current_pipeline()
            .pipeline()
            .get_source_stream(&self.source_stream_name)?;

        let source_row = source_reader
            .lock()
            .expect("other worker threads sharing the same subtask must not get panic")
            .next_row()?;

        source_row.into_row(source_stream.shape())
    }
}
