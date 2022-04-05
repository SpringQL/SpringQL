// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

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
