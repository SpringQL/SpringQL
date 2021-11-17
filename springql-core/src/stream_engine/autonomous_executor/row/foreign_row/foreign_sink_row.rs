use crate::{low_level_rs::SpringRow, stream_engine::autonomous_executor::row::Row};

use super::format::json::JsonObject;

/// Output row into foreign systems (retrieved by SinkServer).
///
/// Immediately converted from Row on stream-engine boundary.
#[derive(PartialEq, Debug)]
pub(crate) struct ForeignSinkRow(Row);

impl From<ForeignSinkRow> for JsonObject {
    fn from(foreign_sink_row: ForeignSinkRow) -> Self {
        let map = foreign_sink_row
            .0
            .into_iter()
            .map(|(col, val)| (col.to_string(), serde_json::Value::from(val)))
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let v = serde_json::Value::from(map);
        JsonObject::new(v)
    }
}

impl From<Row> for ForeignSinkRow {
    fn from(row: Row) -> Self {
        Self(row)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        pipeline::name::ColumnName,
        stream_engine::autonomous_executor::{row::value::sql_value::SqlValue, Timestamp},
    };

    use super::*;

    #[test]
    fn test_into_json() {
        let row = Row::fx_city_temperature_tokyo();
        let f_row = ForeignSinkRow(row);

        let json = JsonObject::new(json!({
            "ts": Timestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21
        }));

        assert_eq!(JsonObject::from(f_row), json);
    }

    #[test]
    fn test_from_row_arrival_rowtime() {
        let row = Row::fx_no_promoted_rowtime();
        let f_row = ForeignSinkRow::from(row);
        let f_json = JsonObject::from(f_row);
        let mut f_colvals = f_json.into_column_values().unwrap();
        let f_rowtime_sql_value = f_colvals.remove(&ColumnName::arrival_rowtime()).unwrap();

        if let SqlValue::NotNull(f_rowtime_nn_sql_value) = f_rowtime_sql_value {
            let f_rowtime: Timestamp = f_rowtime_nn_sql_value.unpack().unwrap();
            assert_eq!(f_rowtime, Timestamp::fx_now());
        } else {
            unreachable!()
        };
    }
}
