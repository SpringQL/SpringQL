use crate::error::Result;
use crate::pipeline::name::ColumnName;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::subtask::query_subtask::final_row::SubtaskRow;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask(Vec<ColumnName>);

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run<DI>(
        &self,
        row: &Row,
    ) -> Result<SubtaskRow>
    where
        DI: DependencyInjection,
    {
        let row = row.projection::<DI>(&self.0)?;
        Ok(SubtaskRow::NewlyCreated(row))
    }
}
