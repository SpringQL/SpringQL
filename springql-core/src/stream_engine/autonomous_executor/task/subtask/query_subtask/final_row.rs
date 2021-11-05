use std::sync::Arc;

use crate::stream_engine::autonomous_executor::row::Row;

#[derive(PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor::task::subtask) enum SubtaskRow {
    /// The same row as task input.
    Preserved(Arc<Row>),

    /// Newly created row during subtask execution.
    NewlyCreated(Row),
}
