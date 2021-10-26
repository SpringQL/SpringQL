use std::{collections::HashMap, rc::Rc};

use crate::{
    dependency_injection::DependencyInjection,
    error::Result,
    model::pipeline::stream_model::stream_shape::StreamShape,
    stream_engine::executor::data::{column::stream_column::StreamColumns, row::Row},
};

use super::format::json::JsonObject;

/// Output row into foreign systems (retrieved by OutputServer).
///
/// Immediately converted from Row on stream-engine boundary.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct ForeignOutputRow(JsonObject);

impl From<ForeignOutputRow> for JsonObject {
    fn from(foreign_output_row: ForeignOutputRow) -> Self {
        foreign_output_row.0
    }
}

/// # Failure
///
/// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
///   - This row cannot be converted into foreign output row.
impl From<Row> for ForeignOutputRow {
    fn from(row: Row) -> Self {
        let map = row
            .into_iter()
            .map(|(col, val)| (col.to_string(), serde_json::Value::from(val)))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let v = serde_json::Value::from(map);
        Self(JsonObject::new(v))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::stream_engine::Timestamp;

    use super::*;

    #[test]
    fn test_from_row() {
        let row = Row::fx_city_temperature_tokyo();

        let f_row = ForeignOutputRow(JsonObject::new(json!({
            "timestamp": Timestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21
        })));

        assert_eq!(ForeignOutputRow::from(row), f_row);
    }
}
