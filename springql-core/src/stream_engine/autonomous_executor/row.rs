// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod value;

pub(in crate::stream_engine::autonomous_executor) mod column;
pub(in crate::stream_engine::autonomous_executor) mod column_values;
pub(in crate::stream_engine) mod foreign_row;
pub(in crate::stream_engine::autonomous_executor) mod row_repository;
pub(in crate::stream_engine::autonomous_executor) mod timestamp;

pub(crate) use foreign_row::ForeignSinkRow;
pub(in crate::stream_engine) use row_repository::{NaiveRowRepository, RowRepository};
pub(crate) use timestamp::{current_timestamp::CurrentTimestamp, Timestamp};

use std::vec;

use self::{column::stream_column::StreamColumns, value::sql_value::SqlValue};
use crate::error::Result;
use crate::pipeline::name::ColumnName;
use crate::stream_engine::autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue;
use crate::stream_engine::dependency_injection::DependencyInjection;

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
    pub(in crate::stream_engine::autonomous_executor) fn new<DI>(cols: StreamColumns) -> Self
    where
        DI: DependencyInjection,
    {
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
    pub(in crate::stream_engine::autonomous_executor) fn projection<DI>(
        &self,
        column_names: &[ColumnName],
    ) -> Result<Self>
    where
        DI: DependencyInjection,
    {
        let new_cols = self.cols.projection(column_names)?;
        Ok(Self::new::<DI>(new_cols))
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

    pub(in crate::stream_engine::autonomous_executor) fn fixme_clone(&self) -> Self {
        Self {
            arrival_rowtime: self.arrival_rowtime,
            cols: self.cols.clone(),
        }
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
