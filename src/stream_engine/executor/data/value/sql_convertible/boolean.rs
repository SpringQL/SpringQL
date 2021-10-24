use crate::{
    error::Result, stream_engine::executor::data::value::sql_value::nn_sql_value::NnSqlValue,
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
