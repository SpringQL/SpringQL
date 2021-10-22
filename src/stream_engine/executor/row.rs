use crate::timestamp::Timestamp;

pub(in crate::stream_engine::executor) mod row_chunk;

/// Row that enables "zero-copy stream".
///
/// - Clone/Copy is disabled.
/// - Immutable. Modification (adding / removing any column or updating column value) leads to new Row.
/// - Mandatory `timestamp` column.
#[derive(Debug)]
struct Row {
    /// Timestamp
    ts: Timestamp,
}
