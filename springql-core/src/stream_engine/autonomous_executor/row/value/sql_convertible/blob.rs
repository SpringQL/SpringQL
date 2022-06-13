// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::Result,
    stream_engine::autonomous_executor::row::value::{
        sql_convertible::{SpringValue, ToNnSqlValue},
        sql_value::NnSqlValue,
    },
};

impl SpringValue for Vec<u8> {
    fn try_from_blob(v: &[u8]) -> Result<Self> {
        Ok(v.to_owned())
    }
}

impl ToNnSqlValue for Vec<u8> {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Blob(self)
    }
}
