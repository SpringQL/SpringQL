use std::sync::Arc;

use crate::stream_engine::autonomous_executor::row::row::Row;

#[derive(PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor::exec) enum FinalRow {
    /// The same row as query plan input.
    Preserved(Arc<Row>),

    /// Newly created row during query plan execution.
    NewlyCreated(Row),
}
