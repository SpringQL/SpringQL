// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod column;
mod column_values;
mod foreign_row;
mod rowtime;
mod value;

pub use column::StreamColumns;
pub use column_values::ColumnValues;
pub use foreign_row::{CANFrameSourceRow, JsonObject, JsonSourceRow, SourceRow, SourceRowFormat};
pub use rowtime::RowTime;
pub use value::{NnSqlValue, SpringValue, SqlCompareResult, SqlValue, SqlValueHashKey};

use std::{sync::Arc, vec};

use crate::{
    api::error::Result,
    mem_size::MemSize,
    pipeline::{ColumnName, StreamModel},
    stream_engine::time::{SpringTimestamp, SystemTimestamp},
};

/// - Mandatory `rowtime()`, either from `cols` or `arrival_rowtime`.
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
        self.cols.get_by_index(i_col)
    }

    /// Creates new row with the different stream model having the same shape.
    ///
    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - `self` and `stream_model` has different shape.
    pub fn apply_new_stream_model(&mut self, stream_model: Arc<StreamModel>) -> Result<()> {
        self.cols.apply_new_stream_model(stream_model)
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
        let into_iter = self.cols.into_iter();
        if let Some(rowtime) = self.processing_time {
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

impl MemSize for StreamRow {
    fn mem_size(&self) -> usize {
        let arrival_rowtime_size = self.processing_time.map_or_else(|| 0, |ts| ts.mem_size());
        let cols_size = self.cols.mem_size();
        arrival_rowtime_size + cols_size
    }
}

impl From<StreamRow> for JsonObject {
    fn from(row: StreamRow) -> Self {
        let map = row
            .into_iter()
            .map(|(col, val)| (col.to_string(), serde_json::Value::from(val)))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let v = serde_json::Value::from(map);
        JsonObject::new(v)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{stream_engine::autonomous_executor::row::foreign_row::JsonObject, time::Duration};

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

    #[test]
    fn test_into_json() {
        let row = StreamRow::fx_city_temperature_tokyo();

        let json = JsonObject::new(json!({
            "ts": SpringTimestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21
        }));

        assert_eq!(JsonObject::from(row), json);
    }

    #[test]
    fn test_from_row_arrival_rowtime() {
        let row = StreamRow::fx_no_promoted_rowtime();
        let f_json = JsonObject::from(row);
        let mut f_colvals = f_json.into_column_values().unwrap();
        let f_rowtime_sql_value = f_colvals.remove(&ColumnName::arrival_rowtime()).unwrap();

        if let SqlValue::NotNull(f_rowtime_nn_sql_value) = f_rowtime_sql_value {
            let f_rowtime: SpringTimestamp = f_rowtime_nn_sql_value.unpack().unwrap();
            assert!(SystemTimestamp::now() - Duration::seconds(1) < f_rowtime);
            assert!(f_rowtime < SystemTimestamp::now() + Duration::seconds(1));
        } else {
            unreachable!()
        };
    }
}
