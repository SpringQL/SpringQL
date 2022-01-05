// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::RowRepository;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct CollectSubtask;

impl CollectSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<Row> {
        context.row_repository().collect_next(&context.task())
    }
}
