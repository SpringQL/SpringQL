use std::rc::Rc;

use serde_json::json;

use crate::{
    dependency_injection::test_di::TestDI,
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        name::{ColumnName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        sql_type::SqlType,
        stream_model::{stream_shape::StreamShape, StreamModel},
    },
    stream_engine::executor::data::{
        column::stream_column::StreamColumns,
        column_values::ColumnValues,
        foreign_input_row::{format::json::JsonObject, ForeignInputRow},
        row::Row,
        timestamp::Timestamp,
        value::sql_value::{nn_sql_value::NnSqlValue, SqlValue},
    },
};

impl Timestamp {
    pub fn fx_now() -> Self {
        "2000-01-01 12:00:00.123456789".parse().unwrap()
    }

    pub fn fx_ts1() -> Self {
        "2021-01-01 13:00:00.000000001".parse().unwrap()
    }
    pub fn fx_ts2() -> Self {
        "2021-01-01 13:00:00.000000002".parse().unwrap()
    }
    pub fn fx_ts3() -> Self {
        "2021-01-01 13:00:00.000000003".parse().unwrap()
    }
}

impl JsonObject {
    pub fn fx_tokyo(ts: Timestamp) -> Self {
        Self::new(json!({
            "timestamp": ts.to_string(),
            "city": "Tokyo",
            "temperature": 21,
        }))
    }

    pub fn fx_osaka(ts: Timestamp) -> Self {
        Self::new(json!({
            "timestamp": ts.to_string(),
            "city": "Osaka",
            "temperature": 23,
        }))
    }

    pub fn fx_london(ts: Timestamp) -> Self {
        Self::new(json!({
            "timestamp": ts.to_string(),
            "city": "London",
            "temperature": 13,
        }))
    }
}

impl ForeignInputRow {
    pub fn fx_tokyo(ts: Timestamp) -> Self {
        Self::from_json(JsonObject::fx_tokyo(ts))
    }
    pub fn fx_osaka(ts: Timestamp) -> Self {
        Self::from_json(JsonObject::fx_osaka(ts))
    }
    pub fn fx_london(ts: Timestamp) -> Self {
        Self::from_json(JsonObject::fx_london(ts))
    }
}

impl StreamShape {
    pub fn fx_city_temperature() -> Self {
        Self::new(
            vec![
                ColumnDefinition::fx_timestamp(),
                ColumnDefinition::fx_city(),
                ColumnDefinition::fx_temperature(),
            ],
            Some(ColumnName::new("timestamp".to_string())),
        )
        .unwrap()
    }
}

impl StreamModel {
    pub fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::new("city_temperature".to_string()),
            Rc::new(StreamShape::fx_city_temperature()),
            Options::empty(),
        )
    }
}

impl Options {
    pub fn empty() -> Self {
        OptionsBuilder::default().build()
    }
}

impl ColumnDefinition {
    pub fn fx_timestamp() -> Self {
        Self::new(ColumnDataType::fx_timestamp())
    }

    pub fn fx_city() -> Self {
        Self::new(ColumnDataType::fx_city())
    }

    pub fn fx_temperature() -> Self {
        Self::new(ColumnDataType::fx_temperature())
    }
}

impl ColumnDataType {
    pub fn fx_timestamp() -> Self {
        Self::new(
            ColumnName::new("timestamp".to_string()),
            SqlType::timestamp(),
            false,
        )
    }

    pub fn fx_city() -> Self {
        Self::new(ColumnName::new("city".to_string()), SqlType::text(), false)
    }

    pub fn fx_temperature() -> Self {
        Self::new(
            ColumnName::new("temperature".to_string()),
            SqlType::integer(),
            false,
        )
    }
}

impl Row {
    pub fn fx_tokyo(ts: Timestamp) -> Self {
        Self::new::<TestDI>(StreamColumns::fx_tokyo(ts))
    }
    pub fn fx_osaka(ts: Timestamp) -> Self {
        Self::new::<TestDI>(StreamColumns::fx_osaka(ts))
    }
    pub fn fx_london(ts: Timestamp) -> Self {
        Self::new::<TestDI>(StreamColumns::fx_london(ts))
    }
}

impl StreamColumns {
    pub fn fx_tokyo(ts: Timestamp) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(ts)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(21)),
            )
            .unwrap();

        Self::new(Rc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }
    pub fn fx_osaka(ts: Timestamp) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(ts)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Osaka".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(23)),
            )
            .unwrap();

        Self::new(Rc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }
    pub fn fx_london(ts: Timestamp) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(ts)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("London".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(13)),
            )
            .unwrap();

        Self::new(Rc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }
}
