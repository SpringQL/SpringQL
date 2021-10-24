use serde_json::json;

use crate::{
    dependency_injection::test_di::TestDI,
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        name::{ColumnName, StreamName},
        sql_type::SqlType,
        stream_model::StreamModel,
    },
    stream_engine::executor::data::{
        column::stream_column::StreamColumns, foreign_input_row::format::json::JsonObject,
        row::Row, timestamp::Timestamp,
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

impl StreamModel {
    pub fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::new("city_temperature".to_string()),
            vec![
                ColumnDefinition::fx_timestamp(),
                ColumnDefinition::fx_city(),
                ColumnDefinition::fx_temperature(),
            ],
            Some(ColumnName::new("timestamp".to_string())),
        )
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
        Self::new::<TestDI>(StreamColumns::fx_tokyo())
    }
}

impl StreamColumns {
    pub fn fx_tokyo() -> Self {}
}
