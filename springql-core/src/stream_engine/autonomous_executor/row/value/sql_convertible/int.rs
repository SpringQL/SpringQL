// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::{anyhow, Context};

use crate::{
    api::error::{Result, SpringError},
    stream_engine::autonomous_executor::row::value::{
        sql_convertible::ToNnSqlValue, sql_value::NnSqlValue, SpringValue,
    },
};

impl SpringValue for i16 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        Ok(*v)
    }

    fn try_from_i32(v: &i32) -> Result<Self> {
        i16::try_from(*v)
            .with_context(|| format!("cannot convert i32 value ({}) into i16", v))
            .map_err(SpringError::Sql)
    }

    fn try_from_i64(v: &i64) -> Result<Self> {
        i16::try_from(*v)
            .with_context(|| format!("cannot convert i64 value ({}) into i16", v))
            .map_err(SpringError::Sql)
    }

    fn try_from_f32(v: &f32) -> Result<Self> {
        Ok(v.ceil() as i16)
    }
}
impl ToNnSqlValue for i16 {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::SmallInt(self)
    }
}

impl SpringValue for i32 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        Ok(*v as i32)
    }

    fn try_from_i32(v: &i32) -> Result<Self> {
        Ok(*v)
    }

    fn try_from_i64(v: &i64) -> Result<Self> {
        i32::try_from(*v)
            .with_context(|| format!("cannot convert i64 value ({}) into i32", v))
            .map_err(SpringError::Sql)
    }

    fn try_from_f32(v: &f32) -> Result<Self> {
        Ok(v.ceil() as i32)
    }
}
impl ToNnSqlValue for i32 {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::Integer(self)
    }
}

impl SpringValue for i64 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        Ok(*v as i64)
    }

    fn try_from_i32(v: &i32) -> Result<Self> {
        Ok(*v as i64)
    }

    fn try_from_i64(v: &i64) -> Result<Self> {
        Ok(*v)
    }

    fn try_from_f32(v: &f32) -> Result<Self> {
        Ok(v.ceil() as i64)
    }
}
impl ToNnSqlValue for i64 {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::BigInt(self)
    }
}

impl SpringValue for u32 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        if 0 <= *v {
            Ok(*v as u32)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i16 value ({}) into u32",
                v
            )))
        }
    }

    fn try_from_i32(v: &i32) -> Result<Self> {
        if 0 <= *v {
            Ok(*v as u32)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i32 value ({}) into u32",
                v
            )))
        }
    }

    fn try_from_i64(v: &i64) -> Result<Self> {
        if 0 <= *v && *v <= u32::MAX as i64 {
            Ok(*v as u32)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i64 value ({}) into u32",
                v
            )))
        }
    }

    fn try_from_u16(v: &u16) -> Result<Self> {
        Ok(*v as u32)
    }

    fn try_from_u32(v: &u32) -> Result<Self> {
        Ok(*v)
    }

    fn try_from_u64(v: &u64) -> Result<Self> {
        u32::try_from(*v)
            .with_context(|| format!("cannot convert u64 value ({}) into u32", v))
            .map_err(SpringError::Sql)
    }
}
impl ToNnSqlValue for u32 {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::UnsignedInteger(self)
    }
}

impl SpringValue for u64 {
    fn try_from_i16(v: &i16) -> Result<Self> {
        if 0 <= *v {
            Ok(*v as u64)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i16 value ({}) into u64",
                v
            )))
        }
    }

    fn try_from_i32(v: &i32) -> Result<Self> {
        if 0 <= *v {
            Ok(*v as u64)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i32 value ({}) into u64",
                v
            )))
        }
    }

    fn try_from_i64(v: &i64) -> Result<Self> {
        if 0 <= *v {
            Ok(*v as u64)
        } else {
            Err(SpringError::Sql(anyhow!(
                "cannot convert i32 value ({}) into u64",
                v
            )))
        }
    }

    fn try_from_u16(v: &u16) -> Result<Self> {
        Ok(*v as u64)
    }

    fn try_from_u32(v: &u32) -> Result<Self> {
        Ok(*v as u64)
    }

    fn try_from_u64(v: &u64) -> Result<Self> {
        Ok(*v)
    }
}
impl ToNnSqlValue for u64 {
    fn into_nn_sql_value(self) -> NnSqlValue {
        NnSqlValue::UnsignedBigInt(self)
    }
}

#[cfg(test)]
mod tests_i32 {
    use crate::{
        api::error::Result, stream_engine::autonomous_executor::row::value::sql_value::NnSqlValue,
    };

    #[test]
    fn test_pack_unpack_i32() -> Result<()> {
        let rust_values = vec![0, 1, -1, i32::MAX, i32::MIN];

        for v in rust_values {
            let sql_value = NnSqlValue::Integer(v);
            let unpacked: i32 = sql_value.unpack()?;
            assert_eq!(unpacked, v);
        }
        Ok(())
    }

    #[test]
    fn test_pack_unpack_i64() {
        let rust_values = vec![0i64, 1, -1, i32::MAX as i64, i32::MIN as i64];

        for v in rust_values {
            let sql_value = NnSqlValue::BigInt(v);
            let unpacked: i32 = sql_value.unpack().unwrap();
            assert_eq!(unpacked, v as i32);
        }
    }
}
