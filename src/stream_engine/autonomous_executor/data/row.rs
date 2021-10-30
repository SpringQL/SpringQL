pub(in crate::stream_engine::autonomous_executor) mod repository;

use std::vec;

use super::{
    column::stream_column::StreamColumns, timestamp::Timestamp, value::sql_value::SqlValue,
};
use crate::error::Result;
use crate::model::name::ColumnName;
use crate::stream_engine::autonomous_executor::data::value::sql_value::nn_sql_value::NnSqlValue;
use crate::stream_engine::dependency_injection::DependencyInjection;

pub(crate) use repository::RowRepository;

#[cfg(test)]
pub(crate) use repository::TestRowRepository;

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
    pub(in crate::stream_engine::autonomous_executor) fn new<DI>(cols: StreamColumns) -> Self
    where
        DI: DependencyInjection,
    {
        use crate::stream_engine::autonomous_executor::data::timestamp::current_timestamp::CurrentTimestamp;

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
    pub(in crate::stream_engine::autonomous_executor) fn rowtime(&self) -> Timestamp {
        self.arrival_rowtime.unwrap_or_else(|| {
            self.cols
                .promoted_rowtime()
                .expect("Either arrival ROWTIME or promoted ROWTIME must be enabled")
        })
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - No column named `column_name` is found from this stream.
    pub(in crate::stream_engine::autonomous_executor) fn get(
        &self,
        column_name: &ColumnName,
    ) -> Result<&SqlValue> {
        self.cols.get(column_name)
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.rowtime().cmp(&other.rowtime()))
    }
}

impl IntoIterator for Row {
    type Item = (ColumnName, SqlValue);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let into_iter = self.cols.into_iter();
        if let Some(rowtime) = self.arrival_rowtime {
            into_iter
                .chain(vec![(
                    ColumnName::arrival_rowtime(),
                    SqlValue::NotNull(NnSqlValue::Timestamp(rowtime)),
                )])
                .collect::<Vec<Self::Item>>()
                .into_iter()
        } else {
            into_iter
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partial_eq() {
        assert_eq!(
            Row::fx_city_temperature_tokyo(),
            Row::fx_city_temperature_tokyo()
        );
    }

    #[test]
    fn test_partial_ne() {
        assert_ne!(
            Row::fx_city_temperature_tokyo(),
            Row::fx_city_temperature_osaka()
        );
    }
}
