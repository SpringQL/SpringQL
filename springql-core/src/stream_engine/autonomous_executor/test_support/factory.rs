// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use springql_foreign_service::source::{ForeignSource, ForeignSourceInput};

use crate::{
    pipeline::{
        test_support::fixture::default_source_reader_config, ColumnName, OptionsBuilder,
        StreamModel, StreamName, StreamShape,
    },
    stream_engine::{
        autonomous_executor::{
            row::{ColumnValues, NnSqlValue, SqlValue, StreamColumns, StreamRow},
            task::{NetClientSourceReader, SourceReader, Tuple},
        },
        time::SpringTimestamp,
    },
};

impl SqlValue {
    pub fn factory_integer(integer: i32) -> Self {
        Self::NotNull(NnSqlValue::factory_integer(integer))
    }

    pub fn factory_bool(bool_: bool) -> Self {
        Self::NotNull(NnSqlValue::factory_bool(bool_))
    }
}

impl NnSqlValue {
    pub fn factory_integer(integer: i32) -> Self {
        Self::Integer(integer)
    }

    pub fn factory_bool(bool_: bool) -> Self {
        Self::Boolean(bool_)
    }
}

impl NetClientSourceReader {
    pub fn factory_with_test_source(input: ForeignSourceInput) -> Self {
        let source = ForeignSource::new().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        source.start(input);
        NetClientSourceReader::start(&options, &default_source_reader_config()).unwrap()
    }
}

impl StreamColumns {
    pub fn factory_city_temperature(
        timestamp: SpringTimestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::fx_timestamp(),
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

        Self::new(Arc::new(StreamModel::fx_city_temperature()), column_values).unwrap()
    }

    pub fn factory_trade(timestamp: SpringTimestamp, ticker: &str, amount: i16) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::fx_timestamp(),
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

        Self::new(Arc::new(StreamModel::fx_trade()), column_values).unwrap()
    }

    pub fn factory_no_promoted_rowtime(amount: i32) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("amount".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(amount)),
            )
            .unwrap();

        Self::new(
            Arc::new(StreamModel::new(
                StreamName::new("st".to_string()),
                StreamShape::fx_no_promoted_rowtime(),
            )),
            column_values,
        )
        .unwrap()
    }
}

impl StreamRow {
    pub fn factory_city_temperature(
        timestamp: SpringTimestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        Self::new(StreamColumns::factory_city_temperature(
            timestamp,
            city,
            temperature,
        ))
    }
    pub fn factory_trade(timestamp: SpringTimestamp, ticker: &str, amount: i16) -> Self {
        Self::new(StreamColumns::factory_trade(timestamp, ticker, amount))
    }
}

impl Tuple {
    pub fn factory_city_temperature(
        timestamp: SpringTimestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        Self::from_row(StreamRow::factory_city_temperature(
            timestamp,
            city,
            temperature,
        ))
    }
    pub fn factory_trade(timestamp: SpringTimestamp, ticker: &str, amount: i16) -> Self {
        Self::from_row(StreamRow::factory_trade(timestamp, ticker, amount))
    }
}
