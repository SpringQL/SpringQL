// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use ordered_float::OrderedFloat;

use crate::{
    api::error::Result,
    stream_engine::autonomous_executor::row::value::{
        sql_convertible::ToNnSqlValue, sql_value::nn_sql_value::NnSqlValue, SpringValue,
    },
};

impl SpringValue for f32 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        Ok(*v as f32)
    }
    fn try_from_i32(v: &i32) -> Result<Self> {
        Ok(*v as f32)
    }
    fn try_from_i64(v: &i64) -> Result<Self> {
        Ok(*v as f32)
    }

    fn try_from_f32(v: &f32) -> Result<Self> {
        Ok(*v)
    }
}

impl ToNnSqlValue for f32 {
    fn into_sql_value(self) -> NnSqlValue {
        NnSqlValue::Float(OrderedFloat(self))
    }
}

#[cfg(test)]
mod tests_f32 {
    use float_cmp::approx_eq;

    use super::*;
    use crate::{
        api::error::Result,
        stream_engine::autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue,
    };

    #[test]
    fn test_pack_unpack_f32() -> Result<()> {
        let rust_values = vec![0f32, 1., -1., f32::MAX, f32::MIN, f32::NAN];

        for v in rust_values {
            let sql_value = NnSqlValue::Float(OrderedFloat(v));
            let unpacked: f32 = sql_value.unpack()?;
            if v.is_nan() {
                assert!(unpacked.is_nan());
            } else {
                assert!(approx_eq!(f32, unpacked, v));
            }
        }
        Ok(())
    }
}
