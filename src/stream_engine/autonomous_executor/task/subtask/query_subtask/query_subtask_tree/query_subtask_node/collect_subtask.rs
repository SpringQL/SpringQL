use std::sync::Arc;

use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct CollectSubtask;

impl CollectSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<Arc<Row>> {
        context.row_repository().collect_next(&context.task())
    }
}
