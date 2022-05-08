// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    error::Result,
    stream_engine::{
        autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue,
        time::timestamp::SpringTimestamp,
    },
};

use super::{SpringValue, ToNnSqlValue};

impl SpringValue for SpringTimestamp {
    fn try_from_string(s: &str) -> Result<Self> {
        s.parse()
    }

    fn try_from_timestamp(v: &SpringTimestamp) -> Result<Self> {
        Ok(*v)
    }
}

impl ToNnSqlValue for SpringTimestamp {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Timestamp(self)
    }
}
