use std::rc::Rc;

use serde_json::json;

use crate::{
    dependency_injection::test_di::TestDI,
    model::{
        column::{column_data_type::ColumnDataType, column_definition::ColumnDefinition},
        name::{ColumnName, PumpName, StreamName},
        option::{options_builder::OptionsBuilder, Options},
        pipeline::stream_model::{stream_shape::StreamShape, StreamModel},
        sql_type::SqlType,
    },
    stream_engine::executor::data::{
        column::stream_column::StreamColumns,
        foreign_row::{
            foreign_input_row::ForeignInputRow, foreign_output_row::ForeignOutputRow,
            format::json::JsonObject,
        },
        row::Row,
        timestamp::Timestamp,
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
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts1().to_string(),
            "city": "Tokyo",
            "temperature": 21,
        }))
    }

    pub fn fx_city_temperature_osaka() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts2().to_string(),
            "city": "Osaka",
            "temperature": 23,
        }))
    }

    pub fn fx_city_temperature_london() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts3().to_string(),
            "city": "London",
            "temperature": 13,
        }))
    }

    pub fn fx_trade_oracle() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts1().to_string(),
            "ticker": "ORCL",
            "amount": 20,
        }))
    }

    pub fn fx_trade_ibm() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts2().to_string(),
            "ticker": "IBM",
            "amount": 30,
        }))
    }

    pub fn fx_trade_google() -> Self {
        Self::new(json!({
            "timestamp": Timestamp::fx_ts3().to_string(),
            "ticker": "GOOGL",
            "amount": 100,
        }))
    }
}

impl ForeignInputRow {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_tokyo())
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_osaka())
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::from_json(JsonObject::fx_city_temperature_london())
    }

    pub fn fx_trade_oracle() -> Self {
        Self::from_json(JsonObject::fx_trade_oracle())
    }
    pub fn fx_trade_ibm() -> Self {
        Self::from_json(JsonObject::fx_trade_ibm())
    }
    pub fn fx_trade_google() -> Self {
        Self::from_json(JsonObject::fx_trade_google())
    }
}

impl ForeignOutputRow {
    pub fn fx_city_temperature_tokyo() -> Self {
        Row::fx_city_temperature_tokyo().into()
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Row::fx_city_temperature_osaka().into()
    }
    pub fn fx_city_temperature_london() -> Self {
        Row::fx_city_temperature_london().into()
    }

    pub fn fx_trade_oracle() -> Self {
        Row::fx_trade_oracle().into()
    }
    pub fn fx_trade_ibm() -> Self {
        Row::fx_trade_ibm().into()
    }
    pub fn fx_trade_google() -> Self {
        Row::fx_trade_google().into()
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
    pub fn fx_ticker() -> Self {
        Self::new(
            vec![
                ColumnDefinition::fx_timestamp(),
                ColumnDefinition::fx_ticker(),
                ColumnDefinition::fx_amount(),
            ],
            Some(ColumnName::new("timestamp".to_string())),
        )
        .unwrap()
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::new(
            vec![ColumnDefinition::fx_amount()],
            None,
        )
        .unwrap()
    }
}

impl StreamModel {
    pub fn fx_city_temperature() -> Self {
        Self::new(
            StreamName::fx_city_temperature(),
            Rc::new(StreamShape::fx_city_temperature()),
            Options::empty(),
        )
    }

    pub fn fx_trade() -> Self {
        Self::new(
            StreamName::fx_trade(),
            Rc::new(StreamShape::fx_ticker()),
            Options::empty(),
        )
    }
}

impl StreamName {
    pub fn fx_city_temperature() -> Self {
        StreamName::new("city_temperature".to_string())
    }
    pub fn fx_trade() -> Self {
        StreamName::new("trade".to_string())
    }
}

impl PumpName {
    pub fn fx_trade_p1() -> Self {
        Self::new("trade_p1".to_string())
    }
    pub fn fx_trade_window() -> Self {
        Self::new("trade_window".to_string())
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

    pub fn fx_ticker() -> Self {
        Self::new(ColumnDataType::fx_ticker())
    }

    pub fn fx_amount() -> Self {
        Self::new(ColumnDataType::fx_amount())
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

    pub fn fx_ticker() -> Self {
        Self::new(
            ColumnName::new("ticker".to_string()),
            SqlType::text(),
            false,
        )
    }

    pub fn fx_amount() -> Self {
        Self::new(
            ColumnName::new("amount".to_string()),
            SqlType::small_int(),
            false,
        )
    }
}

impl Row {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_tokyo())
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_osaka())
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_city_temperature_london())
    }

    pub fn fx_trade_oracle() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_oracle())
    }
    pub fn fx_trade_ibm() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_ibm())
    }
    pub fn fx_trade_google() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_trade_google())
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::new::<TestDI>(StreamColumns::fx_no_promoted_rowtime())
    }
}

impl StreamColumns {
    pub fn fx_city_temperature_tokyo() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts1(), "Tokyo", 21)
    }
    pub fn fx_city_temperature_osaka() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts2(), "Osaka", 23)
    }
    pub fn fx_city_temperature_london() -> Self {
        Self::factory_city_temperature(Timestamp::fx_ts3(), "London", 13)
    }

    pub fn fx_trade_oracle() -> Self {
        Self::factory_trade(Timestamp::fx_ts1(), "ORCL", 20)
    }
    pub fn fx_trade_ibm() -> Self {
        Self::factory_trade(Timestamp::fx_ts2(), "IBM", 30)
    }
    pub fn fx_trade_google() -> Self {
        Self::factory_trade(Timestamp::fx_ts3(), "GOOGL", 100)
    }

    pub fn fx_no_promoted_rowtime() -> Self {
        Self::factory_no_promoted_rowtime(12345)
    }
}
