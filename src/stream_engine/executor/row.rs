use crate::timestamp::Timestamp;

pub(in crate::stream_engine::executor) mod row_chunk;

/// Row that enables "zero-copy stream".
///
/// - Clone/Copy is disabled.
/// - Immutable. Modification (adding / removing any column or updating column value) leads to new Row.
/// - Mandatory `timestamp` column.
/// - Ord by timestamp.
/// - Eq by all columns.
#[derive(Eq, PartialEq, Debug)]
pub(super) struct Row {
    /// Timestamp
    ts: Timestamp,
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts.cmp(&other.ts)
    }
}
impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
