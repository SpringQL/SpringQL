// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::Result,
    stream_engine::autonomous_executor::row::value::{
        sql_convertible::{SpringValue, ToNnSqlValue},
        sql_value::NnSqlValue,
    },
};

impl SpringValue for bool {
    fn try_from_bool(v: &bool) -> Result<Self> {
        Ok(*v)
    }
}

impl ToNnSqlValue for bool {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::Boolean(self)
    }
}
