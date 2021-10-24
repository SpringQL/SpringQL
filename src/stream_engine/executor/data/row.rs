mod repository;

use crate::timestamp::Timestamp;

use super::column::stream_column::StreamColumns;

/// Row that enables "zero-copy stream".
///
/// - Clone/Copy is disabled.
/// - Immutable. Modification (adding / removing any column or updating column value) leads to new Row.
/// - Mandatory `timestamp` column.
/// - Ord by timestamp.
/// - Eq by all columns.
#[derive(Eq, PartialEq, Debug)]
pub(super) struct Row {
    arrival_rowtime: Option<Timestamp>,

    /// Columns
    cols: StreamColumns,
}

impl Row {
    /// ROWTIME. See: <https://docs.sqlstream.com/glossary/rowtime-gl/>
    ///
    /// ROWTIME is a:
    ///
    /// - (default) Arrival time to a stream.
    /// - Promoted from a column in a stream.
    pub(super) fn rowtime(&self) -> Timestamp {
        self.arrival_rowtime.unwrap_or_else(|| {
            *self
                .cols
                .promoted_rowtime()
                .expect("Either arrival ROWTIME or promoted ROWTIME must be enabled")
        })
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rowtime().cmp(&other.rowtime())
    }
}
impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
