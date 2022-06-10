// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub use value::SpringValue;

pub mod value;

pub mod column;
pub mod column_values;
pub mod foreign_row;

use std::vec;

use crate::{
    api::error::Result,
    mem_size::MemSize,
    pipeline::{ColumnName, StreamModel},
    stream_engine::{
        autonomous_executor::row::{
            column::stream_column::StreamColumns,
            foreign_row::format::json::JsonObject,
            value::sql_value::{nn_sql_value::NnSqlValue, SqlValue},
        },
        time::timestamp::{system_timestamp::SystemTimestamp, SpringTimestamp},
    },
};

/// - Mandatory `rowtime()`, either from `cols` or `arrival_rowtime`.
/// - PartialEq by all columns (NULL prevents Eq).
/// - PartialOrd by timestamp.
#[derive(Clone, PartialEq, Debug)]
pub struct Row {
    arrival_rowtime: Option<SpringTimestamp>,

    /// Columns
    cols: StreamColumns,
}

impl Row {
    pub fn new(cols: StreamColumns) -> Self {
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

    pub fn stream_model(&self) -> &StreamModel {
        self.cols.stream_model()
    }

    /// ROWTIME. See: <https://docs.sqlstream.com/glossary/rowtime-gl/>
    ///
    /// ROWTIME is a:
    ///
    /// - (default) Arrival time to a stream.
    /// - Promoted from a column in a stream.
    pub fn rowtime(&self) -> SpringTimestamp {
        self.arrival_rowtime.unwrap_or_else(|| {
            self.cols
                .promoted_rowtime()
                .expect("Either arrival ROWTIME or promoted ROWTIME must be enabled")
        })
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - Column index out of range
    pub fn get_by_index(&self, i_col: usize) -> Result<&SqlValue> {
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

impl From<Row> for JsonObject {
    fn from(row: Row) -> Self {
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

    use crate::{
        stream_engine::autonomous_executor::row::foreign_row::format::json::JsonObject,
        time::Duration,
    };

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

    #[test]
    fn test_into_json() {
        let row = Row::fx_city_temperature_tokyo();

        let json = JsonObject::new(json!({
            "ts": SpringTimestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21
        }));

        assert_eq!(JsonObject::from(row), json);
    }

    #[test]
    fn test_from_row_arrival_rowtime() {
        let row = Row::fx_no_promoted_rowtime();
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
