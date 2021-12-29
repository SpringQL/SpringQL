// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod sink_subtask;
pub(in crate::stream_engine::autonomous_executor) mod source_subtask;

use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::pipeline::name::SourceReaderName;
use crate::pipeline::pipeline_graph::PipelineGraph;
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::dependency_injection::DependencyInjection;

use super::task_context::TaskContext;
use super::task_id::TaskId;
use super::task_state::TaskState;

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    state: TaskState,
    source_reader_name: SourceReaderName,
    downstream: Arc<ForeignStreamModel>,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(
        source_reader: &SourceReaderModel,
        pipeline_graph: &PipelineGraph,
    ) -> Self {
        let id = TaskId::from_source_reader(source_reader.dest_foreign_stream().name().clone());
        let downstream = source_reader.dest_foreign_stream();
        Self {
            id,
            state: TaskState::from(&source_reader.state(pipeline_graph)),
            source_reader_name: source_reader.name().clone(),
            downstream,
        }
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine) fn state(&self) -> &TaskState {
        &self.state
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        debug_assert!(!context.downstream_tasks().is_empty());

        let row = self.collect_next::<DI>(context)?;
        context
            .row_repository()
            .emit(row, &context.downstream_tasks())
    }

    fn collect_next<DI: DependencyInjection>(&self, context: &TaskContext<DI>) -> Result<Row> {
        let source_reader = context
            .source_subtask_repository()
            .get_source_subtask(&self.source_reader_name);

        let foreign_row = source_reader
            .lock()
            .expect("other worker threads sharing the same subtask must not get panic")
            .next_row()?;
        foreign_row.into_row::<DI>(self.downstream.shape())
    }
}
