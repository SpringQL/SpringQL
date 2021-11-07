use std::sync::Arc;

use crate::error::Result;
use crate::pipeline::name::ColumnName;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask(Vec<ColumnName>);

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        row: &Row,
    ) -> Result<Row> {
        row.projection(&self.0)
    }
}
