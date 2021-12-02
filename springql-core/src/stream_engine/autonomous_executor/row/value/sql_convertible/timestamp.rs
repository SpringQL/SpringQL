// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::Result,
    stream_engine::autonomous_executor::{
        row::value::sql_value::nn_sql_value::NnSqlValue, Timestamp,
    },
};

use super::SqlConvertible;

impl SqlConvertible for Timestamp {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Timestamp(self)
    }

    fn try_from_string(s: &str) -> Result<Self> {
        s.parse()
    }

    fn try_from_timestamp(v: &Timestamp) -> Result<Self> {
        Ok(*v)
    }
}
