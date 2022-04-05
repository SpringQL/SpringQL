// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod value;

pub(in crate::stream_engine::autonomous_executor) mod column;
pub(in crate::stream_engine::autonomous_executor) mod column_values;
pub(in crate::stream_engine) mod foreign_row;

pub(crate) use foreign_row::SinkRow;

use std::vec;

use self::{column::stream_column::StreamColumns, value::sql_value::SqlValue};
use crate::error::Result;
use crate::mem_size::MemSize;
use crate::pipeline::name::ColumnName;
use crate::pipeline::stream_model::StreamModel;
use crate::stream_engine::autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue;
use crate::stream_engine::time::timestamp::system_timestamp::SystemTimestamp;
use crate::stream_engine::time::timestamp::Timestamp;

/// - Mandatory `rowtime()`, either from `cols` or `arrival_rowtime`.
/// - PartialEq by all columns (NULL prevents Eq).
/// - PartialOrd by timestamp.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Row {
    arrival_rowtime: Option<Timestamp>,

    /// Columns
    cols: StreamColumns,
}

impl Row {
    pub(in crate::stream_engine::autonomous_executor) fn new(cols: StreamColumns) -> Self {
        let arrival_rowtime = if cols.promoted_rowtime().is_some() {
            None
        } else {
            Some(SystemTimestamp::now())
        };

        Row {
            arrival_rowtime,
            cols,
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn stream_model(&self) -> &StreamModel {
        self.cols.stream_model()
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
    ///   - Column index out of range
    pub(in crate::stream_engine::autonomous_executor) fn get_by_index(
        &self,
        i_col: usize,
    ) -> Result<&SqlValue> {
        self.cols.get_by_index(i_col)
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

impl MemSize for Row {
    fn mem_size(&self) -> usize {
        let arrival_rowtime_size = self.arrival_rowtime.map_or_else(|| 0, |ts| ts.mem_size());
        let cols_size = self.cols.mem_size();
        arrival_rowtime_size + cols_size
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
