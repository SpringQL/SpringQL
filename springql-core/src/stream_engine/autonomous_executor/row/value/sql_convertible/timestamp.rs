// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::Result,
    stream_engine::{
        autonomous_executor::row::value::{
            sql_convertible::ToNnSqlValue, sql_value::NnSqlValue, SpringValue,
        },
        time::SpringTimestamp,
    },
};

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
