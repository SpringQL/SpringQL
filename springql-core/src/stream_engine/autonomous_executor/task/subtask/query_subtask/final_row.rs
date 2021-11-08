use std::sync::Arc;

use crate::stream_engine::autonomous_executor::row::Row;

#[derive(PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) enum SubtaskRow {
    /// The same row as task input.
    Preserved(Arc<Row>),

    /// Newly created row during subtask execution.
    NewlyCreated(Row),
}

impl AsRef<Row> for SubtaskRow {
    fn as_ref(&self) -> &Row {
        match self {
            SubtaskRow::Preserved(row) => row.as_ref(),
            SubtaskRow::NewlyCreated(row) => row,
        }
    }
}
