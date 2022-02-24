// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::Result,
    stream_engine::autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue,
};

use super::SqlConvertible;

impl SqlConvertible for bool {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Boolean(self)
    }

    fn try_from_bool(v: &bool) -> Result<Self> {
        Ok(*v)
    }
}
