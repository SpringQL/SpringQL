// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::Result,
    stream_engine::{
        autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue,
        time::duration::event_duration::EventDuration,
    },
};

use super::SqlConvertible;

impl SqlConvertible for EventDuration {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Duration(self)
    }

    fn try_from_duration(v: &EventDuration) -> Result<Self> {
        Ok(*v)
    }
}
