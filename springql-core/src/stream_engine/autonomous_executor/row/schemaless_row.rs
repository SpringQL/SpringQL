// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::vec;

use crate::{
    api::error::Result,
    pipeline::ColumnName,
    stream_engine::{
        autonomous_executor::{ColumnValues, JsonObject},
        SqlValue, StreamRow,
    },
};

/// Row untied with a stream.
/// Used for foreign sources and sinks.
///
/// - PartialEq by all columns (NULL prevents Eq).
#[derive(Clone, PartialEq, Debug)]
pub struct SchemalessRow {
    /// Columns
    colvals: ColumnValues,
}

impl SchemalessRow {
    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - Column index out of range
    pub fn get_by_index(&self, i_col: usize) -> Result<&SqlValue> {
        self.colvals.get_by_index(i_col)
    }

    pub fn into_column_values(self) -> ColumnValues {
        self.colvals
    }
}

impl From<StreamRow> for SchemalessRow {
    fn from(stream_row: StreamRow) -> Self {
        let colvals = stream_row.into();
        Self { colvals }
    }
}

impl From<ColumnValues> for SchemalessRow {
    fn from(colvals: ColumnValues) -> Self {
        Self { colvals }
    }
}

impl IntoIterator for SchemalessRow {
    type Item = (ColumnName, SqlValue);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.colvals.into_iter()
    }
}

impl From<SchemalessRow> for JsonObject {
    fn from(row: SchemalessRow) -> Self {
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

    use crate::stream_engine::time::SpringTimestamp;

    use super::*;

    #[test]
    fn test_into_json() {
        let row = SchemalessRow::fx_city_temperature_tokyo();

        let json = JsonObject::new(json!({
            "ts": SpringTimestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21
        }));

        assert_eq!(JsonObject::from(row), json);
    }
}
