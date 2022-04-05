// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

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
