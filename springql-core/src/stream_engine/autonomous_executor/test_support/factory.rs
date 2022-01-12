// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use springql_foreign_service::source::{source_input::ForeignSourceInput, ForeignSource};

use crate::low_level_rs::SpringSourceReaderConfig;
use crate::stream_engine::autonomous_executor::task::source_task::source_reader::net::NetSourceReader;
use crate::stream_engine::autonomous_executor::task::source_task::source_reader::SourceReader;
use crate::stream_engine::time::timestamp::Timestamp;
use crate::{
    pipeline::{
        name::ColumnName, option::options_builder::OptionsBuilder,
        stream_model::stream_shape::StreamShape,
    },
    stream_engine::autonomous_executor::row::{
        column::stream_column::StreamColumns,
        column_values::ColumnValues,
        value::sql_value::{nn_sql_value::NnSqlValue, SqlValue},
        Row,
    },
};

impl NetSourceReader {
    pub(in crate::stream_engine) fn factory_with_test_source(input: ForeignSourceInput) -> Self {
        let source = ForeignSource::start(input).unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", source.host_ip().to_string())
            .add("REMOTE_PORT", source.port().to_string())
            .build();

        NetSourceReader::start(&options, &SpringSourceReaderConfig::fx_default()).unwrap()
    }
}

impl StreamColumns {
    pub(in crate::stream_engine) fn factory_city_temperature(
        timestamp: Timestamp,
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

        Self::new(Arc::new(StreamShape::fx_city_temperature()), column_values).unwrap()
    }

    pub(in crate::stream_engine) fn factory_trade(
        timestamp: Timestamp,
        ticker: &str,
        amount: i16,
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

        Self::new(Arc::new(StreamShape::fx_trade()), column_values).unwrap()
    }

    pub(in crate::stream_engine) fn factory_no_promoted_rowtime(amount: i32) -> Self {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("amount".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(amount)),
            )
            .unwrap();

        Self::new(
            Arc::new(StreamShape::fx_no_promoted_rowtime()),
            column_values,
        )
        .unwrap()
    }
}

impl Row {
    pub(in crate::stream_engine) fn factory_city_temperature(
        timestamp: Timestamp,
        city: &str,
        temperature: i32,
    ) -> Self {
        Self::new(StreamColumns::factory_city_temperature(
            timestamp,
            city,
            temperature,
        ))
    }
    pub(in crate::stream_engine) fn factory_trade(
        timestamp: Timestamp,
        ticker: &str,
        amount: i16,
    ) -> Self {
        Self::new(StreamColumns::factory_trade(timestamp, ticker, amount))
    }
}
