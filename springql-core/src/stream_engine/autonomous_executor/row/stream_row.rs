// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{sync::Arc, vec};

use crate::{
    api::error::Result,
    mem_size::MemSize,
    pipeline::{ColumnName, StreamModel},
    stream_engine::{
        autonomous_executor::{row::schemaless_row::SchemalessRow, ColumnValues, StreamColumns},
        time::{SpringTimestamp, SystemTimestamp},
        RowTime, SqlValue,
    },
};

/// - Mandatory `rowtime()`, either from `cols` or `ptime`.
/// - PartialEq by all columns (NULL prevents Eq).
/// - PartialOrd by `rowtime()`.
#[derive(Clone, PartialEq, Debug)]
pub struct StreamRow {
    /// None if an event time is available (i.e. ROWTIME keyword is supplied)
    processing_time: Option<SpringTimestamp>,

    /// Columns
    cols: StreamColumns,
}

impl StreamRow {
    pub fn new(cols: StreamColumns) -> Self {
        let processing_time = if cols.event_time().is_some() {
            None
        } else {
            Some(SystemTimestamp::now())
        };

        StreamRow {
            processing_time,
            cols,
        }
    }

    pub fn from_schemaless_row(row: SchemalessRow, stream_model: Arc<StreamModel>) -> Result<Self> {
        let cols = StreamColumns::new(stream_model, row.into_column_values())?;
        Ok(Self::new(cols))
    }

    pub fn stream_model(&self) -> &StreamModel {
        self.cols.stream_model()
    }

    /// ROWTIME. See: <https://docs.sqlstream.com/glossary/rowtime-gl/>
    ///
    /// ROWTIME is a:
    ///
    /// - (default) Arrival time to a stream.
    /// - Promoted from a column in a stream.
    pub fn rowtime(&self) -> RowTime {
        self.processing_time.map_or_else(
            || {
                RowTime::EventTime(
                    self.cols
                        .event_time()
                        .expect("Either processing time or event time must be enabled"),
                )
            },
            RowTime::ProcessingTime,
        )
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - Column index out of range
    pub fn get_by_index(&self, i_col: usize) -> Result<&SqlValue> {
        self.cols.get_by_index(i_col) // TODO rm all
    }
}

impl PartialOrd for StreamRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.rowtime().cmp(&other.rowtime()))
    }
}

impl IntoIterator for StreamRow {
    type Item = (ColumnName, SqlValue);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.cols.into_iter()
    }
}

impl MemSize for StreamRow {
    fn mem_size(&self) -> usize {
        let ptime_size = self.processing_time.map_or_else(|| 0, |ts| ts.mem_size());
        let cols_size = self.cols.mem_size();
        ptime_size + cols_size
    }
}

impl From<StreamRow> for ColumnValues {
    fn from(row: StreamRow) -> Self {
        let cols = row.cols;
        cols.into()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_partial_eq() {
        assert_eq!(
            StreamRow::fx_city_temperature_tokyo(),
            StreamRow::fx_city_temperature_tokyo()
        );
    }

    #[test]
    fn test_partial_ne() {
        assert_ne!(
            StreamRow::fx_city_temperature_tokyo(),
            StreamRow::fx_city_temperature_osaka()
        );
    }
}
