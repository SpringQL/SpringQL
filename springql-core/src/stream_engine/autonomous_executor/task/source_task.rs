// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod source_reader;

use std::fmt::Debug;

use crate::error::Result;
use crate::pipeline::name::{SourceReaderName, StreamName};
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;

use super::task_context::TaskContext;

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    source_reader_name: SourceReaderName,
    source_stream_name: StreamName,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(source_reader: &SourceReaderModel) -> Self {
        let id = TaskId::from_source(source_reader);
        Self {
            id,
            source_reader_name: source_reader.name().clone(),
            source_stream_name: source_reader.dest_source_stream().clone(),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<()> {
        let row = self.collect_next(context)?;
        let repos = context.repos();

        let out_qid = context
            .output_queues()
            .first()
            .expect("source task must have 1 output queue")
            .clone();
        match out_qid {
            QueueId::Row(qid) => {
                let row_q_repo = repos.row_queue_repository();
                let queue = row_q_repo.get(&qid);
                queue.put(row);
                Ok(())
            }
            QueueId::Window(_) => todo!(),
        }
    }

    fn collect_next(&self, context: &TaskContext) -> Result<Row> {
        let source_reader = context
            .repos()
            .source_reader_repository()
            .get_source_reader(&self.source_reader_name);

        let source_stream = context
            .pipeline_derivatives()
            .pipeline()
            .get_source_stream(&self.source_stream_name)?;

        let source_row = source_reader
            .lock()
            .expect("other worker threads sharing the same subtask must not get panic")
            .next_row()?;

        source_row.into_row(source_stream.shape())
    }
}
