pub(in crate::stream_engine::executor) mod repository;

use super::{column::stream_column::StreamColumns, timestamp::Timestamp};
use crate::dependency_injection::DependencyInjection;

/// Row that enables "zero-copy stream".
///
/// - Clone/Copy is disabled.
/// - Immutable. Modification (adding / removing any column or updating column value) leads to new Row.
/// - Mandatory `timestamp` column.
/// - Ord by timestamp.
/// - Eq by all columns.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct Row {
    arrival_rowtime: Option<Timestamp>,

    /// Columns
    cols: StreamColumns,
}

impl Row {
    pub(in crate::stream_engine::executor) fn new<DI>(cols: StreamColumns) -> Self
    where
        DI: DependencyInjection,
    {
        use crate::stream_engine::executor::data::timestamp::current_timestamp::CurrentTimestamp;

        let arrival_rowtime = if cols.promoted_rowtime().is_some() {
            None
        } else {
            Some(DI::CurrentTimestampType::now())
        };

        Row {
            arrival_rowtime,
            cols,
        }
    }

    /// ROWTIME. See: <https://docs.sqlstream.com/glossary/rowtime-gl/>
    ///
    /// ROWTIME is a:
    ///
    /// - (default) Arrival time to a stream.
    /// - Promoted from a column in a stream.
    pub(in crate::stream_engine::executor) fn rowtime(&self) -> Timestamp {
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
