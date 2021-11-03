use std::sync::Arc;

use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::dependency_injection::DependencyInjection;
use crate::stream_engine::RowRepository;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) struct CollectExecutor;

impl CollectExecutor {
    pub(in crate::stream_engine::autonomous_executor::exec::query_executor) fn run<
        DI: DependencyInjection,
    >(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<Arc<Row>> {
        context.row_repository().collect_next(&context.task())
    }
}
