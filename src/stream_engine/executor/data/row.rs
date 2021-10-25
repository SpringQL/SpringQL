pub(in crate::stream_engine::executor) mod repository;

pub(crate) use repository::{RefCntGcRowRepository, RowRepository};

use super::{column::stream_column::StreamColumns, timestamp::Timestamp};
use crate::dependency_injection::DependencyInjection;

/// Row that enables "zero-copy stream".
///
/// - Clone/Copy is disabled.
/// - Immutable. Modification (adding / removing any column or updating column value) leads to new Row.
/// - Mandatory `rowtime()`, either from `cols` or `arrival_rowtime`.
/// - PartialEq by all columns (NULL prevents Eq).
/// - PartialOrd by timestamp.
#[derive(PartialEq, Debug)]
pub(crate) struct Row {
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
            self.cols
                .promoted_rowtime()
                .expect("Either arrival ROWTIME or promoted ROWTIME must be enabled")
        })
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.rowtime().cmp(&other.rowtime()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_eq() {
        assert_eq!(
            Row::fx_tokyo(Timestamp::fx_ts1()),
            Row::fx_tokyo(Timestamp::fx_ts1())
        );
    }

    #[test]
    fn test_partial_ne_timestamp() {
        assert_ne!(
            Row::fx_tokyo(Timestamp::fx_ts1()),
            Row::fx_tokyo(Timestamp::fx_ts2())
        );
    }
}
