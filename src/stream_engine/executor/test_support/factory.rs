use std::rc::Rc;

use crate::{
    dependency_injection::test_di::TestDI,
    model::{
        name::ColumnName, option::options_builder::OptionsBuilder,
        pipeline::stream_model::stream_shape::StreamShape,
    },
    stream_engine::{
        executor::{
            data::{
                column::stream_column::StreamColumns,
                column_values::ColumnValues,
                foreign_input_row::format::json::JsonObject,
                row::Row,
                value::sql_value::{nn_sql_value::NnSqlValue, SqlValue},
            },
            server::input::{
                net::{NetInputServerActive, NetInputServerStandby},
                InputServerStandby,
            },
            test_support::foreign::source::TestSource,
        },
        Timestamp,
    },
};

impl NetInputServerActive {
    pub fn factory_with_test_source(inputs: Vec<JsonObject>) -> Self {
        let source = TestSource::start(inputs).unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        let server = NetInputServerStandby::new(options).unwrap();
        server.start().unwrap()
    }
}

impl StreamColumns {
    pub fn factory_city_temperature(timestamp: Timestamp, city: &str, temperature: i32) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(timestamp)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text(city.to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(temperature)),
            )
            .unwrap();

        Self::new(Rc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }

    pub fn factory_ticker(timestamp: Timestamp, ticker: &str, amount: i16) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(timestamp)),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("ticker".to_string()),
                SqlValue::NotNull(NnSqlValue::Text(ticker.to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("amount".to_string()),
                SqlValue::NotNull(NnSqlValue::SmallInt(amount)),
            )
            .unwrap();

        Self::new(Rc::new(StreamShape::fx_ticker()), column_values).unwrap()
    }
}

impl Row {
    pub fn factory_city_temperature(timestamp: Timestamp, city: &str, temperature: i32) -> Self {
        Self::new::<TestDI>(StreamColumns::factory_city_temperature(
            timestamp,
            city,
            temperature,
        ))
    }
    pub fn factory_ticker(timestamp: Timestamp, ticker: &str, amount: i16) -> Self {
        Self::new::<TestDI>(StreamColumns::factory_ticker(timestamp, ticker, amount))
    }
}
