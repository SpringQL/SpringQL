use crate::{
    error::Result, stream_engine::executor::data::value::sql_value::nn_sql_value::NnSqlValue,
};

use super::SqlConvertible;

impl SqlConvertible for String {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Text(self)
    }

    fn try_from_string(v: &str) -> Result<Self> {
        Ok(v.to_string())
    }
}
