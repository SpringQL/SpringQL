// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    fmt::Display,
    hash::Hash,
    mem::size_of,
    ops::{Add, Mul},
};

use anyhow::anyhow;
use ordered_float::OrderedFloat;

use crate::{
    api::error::{Result, SpringError},
    mem_size::MemSize,
    pipeline::{
        F32LooseType, I64LooseType, NumericComparableType, SqlType, StringComparableLoseType,
        U64LooseType,
    },
    stream_engine::{
        autonomous_executor::row::value::{
            sql_convertible::ToNnSqlValue, sql_value::sql_compare_result::SqlCompareResult,
        },
        time::{SpringEventDuration, SpringTimestamp},
        SpringValue,
    },
};

/// NOT NULL value.
#[derive(Clone, Debug)]
pub enum NnSqlValue {
    /// SMALLINT
    SmallInt(i16),
    /// INTEGER
    Integer(i32),
    /// BIGINT
    BigInt(i64),

    /// UNSIGNED INTEGER
    UnsignedInteger(u32),
    /// UNSIGNED BIGINT
    UnsignedBigInt(u64),

    /// FLOAT
    Float(
        // to implement Hash
        OrderedFloat<f32>,
    ),

    /// TEXT
    Text(String),

    /// BLOB
    Blob(Vec<u8>),

    /// BOOLEAN
    Boolean(bool),

    /// TIMESTAMP
    Timestamp(SpringTimestamp),

    /// DURATION
    Duration(SpringEventDuration),
}

impl MemSize for NnSqlValue {
    fn mem_size(&self) -> usize {
        match self {
            NnSqlValue::SmallInt(_) => size_of::<i16>(),
            NnSqlValue::Integer(_) => size_of::<i32>(),
            NnSqlValue::BigInt(_) => size_of::<i64>(),

            NnSqlValue::UnsignedInteger(_) => size_of::<u32>(),
            NnSqlValue::UnsignedBigInt(_) => size_of::<u64>(),

            NnSqlValue::Float(_) => size_of::<f32>(),

            NnSqlValue::Text(s) => s.capacity(),
            NnSqlValue::Blob(v) => v.capacity(),

            NnSqlValue::Boolean(_) => size_of::<bool>(),

            NnSqlValue::Timestamp(ts) => ts.mem_size(),

            NnSqlValue::Duration(dur) => dur.mem_size(),
        }
    }
}

/// Although function is better to use,
///
/// ```ignore
/// fn for_all_loose_types<R, FnNull, FnI64, FnString>(
///     &self,
///     f_i64: FnI64,
///     f_string: FnString,
/// ) -> R
/// where
///     FnI64: FnOnce(i64) -> R,
///     FnString: FnOnce(String) -> R,
/// ```
///
/// does not work properly with closures which capture &mut environments.
macro_rules! for_all_loose_types {
    ( $nn_sql_value:expr, $closure_i64:expr, $closure_u64:expr, $closure_ordered_float:expr, $closure_string:expr, $closure_blob:expr, $closure_bool:expr, $closure_timestamp:expr, $closure_duration:expr ) => {{
        match &$nn_sql_value {
            NnSqlValue::SmallInt(_) | NnSqlValue::Integer(_) | NnSqlValue::BigInt(_) => {
                let v = $nn_sql_value.unpack::<i64>().unwrap();
                $closure_i64(v)
            }
            NnSqlValue::UnsignedInteger(_) | NnSqlValue::UnsignedBigInt(_) => {
                let v = $nn_sql_value.unpack::<u64>().unwrap();
                $closure_u64(v)
            }
            NnSqlValue::Float(_) => {
                let v = $nn_sql_value.unpack::<f32>().unwrap();
                $closure_ordered_float(OrderedFloat(v))
            }
            NnSqlValue::Text(s) => $closure_string(s.to_string()),
            NnSqlValue::Blob(v) => $closure_blob(v.to_owned()),
            NnSqlValue::Boolean(b) => $closure_bool(b.clone()),
            NnSqlValue::Timestamp(t) => $closure_timestamp(*t),
            NnSqlValue::Duration(d) => $closure_duration(*d),
        }
    }};
}

impl PartialEq for NnSqlValue {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.sql_compare(other), Ok(SqlCompareResult::Eq))
    }
}
impl Eq for NnSqlValue {}

impl Hash for NnSqlValue {
    /// Although raw format are different between two NnSqlValue, this hash function must return the same value if loosely typed values are the same.
    /// E.g. `42 SMALLINT`'s hash value must be equal to that of `42 INTEGER`.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for_all_loose_types!(
            self,
            |i: i64| {
                i.hash(state);
            },
            |u: u64| {
                u.hash(state);
            },
            |f: OrderedFloat<f32>| {
                f.hash(state);
            },
            |s: String| {
                s.hash(state);
            },
            |v: Vec<u8>| {
                v.hash(state);
            },
            |b: bool| { b.hash(state) },
            |t: SpringTimestamp| { t.hash(state) },
            |d: SpringEventDuration| { d.hash(state) }
        )
    }
}

impl Display for NnSqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = for_all_loose_types!(
            self,
            |i: i64| i.to_string(),
            |u: u64| u.to_string(),
            |f: OrderedFloat<f32>| f.to_string(),
            |s: String| format!(r#""{}""#, s),
            |v: Vec<u8>| format!("{:?}", v),
            |b: bool| (if b { "TRUE" } else { "FALSE" }).to_string(),
            |t: SpringTimestamp| t.to_string(),
            |d: SpringEventDuration| d.to_string()
        );
        write!(f, "{}", s)
    }
}

impl NnSqlValue {
    /// Retrieve Rust value.
    ///
    /// Allows "loosely-get", which captures a value into a looser type.
    /// E.g. unpack() `NnSqlValue::SmallInt(1)` into `i32`.
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - Any value of `T` cannot be typed as this SqlValue's SqlType (E.g. `T = i64`, `SqlType = SmallInt`).
    pub fn unpack<T>(&self) -> Result<T>
    where
        T: SpringValue,
    {
        match self {
            NnSqlValue::SmallInt(i16_) => T::try_from_i16(i16_),
            NnSqlValue::Integer(i32_) => T::try_from_i32(i32_),
            NnSqlValue::BigInt(i64_) => T::try_from_i64(i64_),
            NnSqlValue::UnsignedInteger(u32_) => T::try_from_u32(u32_),
            NnSqlValue::UnsignedBigInt(u64_) => T::try_from_u64(u64_),
            NnSqlValue::Float(f32_) => T::try_from_f32(f32_),
            NnSqlValue::Text(string) => T::try_from_string(string),
            NnSqlValue::Blob(blob) => T::try_from_blob(blob),
            NnSqlValue::Boolean(b) => T::try_from_bool(b),
            NnSqlValue::Timestamp(t) => T::try_from_timestamp(t),
            NnSqlValue::Duration(d) => T::try_from_duration(d),
        }
    }

    /// SqlType of this value
    pub fn sql_type(&self) -> SqlType {
        match self {
            NnSqlValue::SmallInt(_) => SqlType::small_int(),
            NnSqlValue::Integer(_) => SqlType::integer(),
            NnSqlValue::BigInt(_) => SqlType::big_int(),
            NnSqlValue::UnsignedInteger(_) => SqlType::unsigned_integer(),
            NnSqlValue::UnsignedBigInt(_) => SqlType::unsigned_big_int(),
            NnSqlValue::Float(_) => SqlType::float(),
            NnSqlValue::Text(_) => SqlType::text(),
            NnSqlValue::Blob(_) => SqlType::blob(),
            NnSqlValue::Boolean(_) => SqlType::boolean(),
            NnSqlValue::Timestamp(_) => SqlType::timestamp(),
            NnSqlValue::Duration(_) => SqlType::duration(),
        }
    }

    /// Try to convert value into a type.
    ///
    /// ```text
    /// SqlValue -- (unpack by typ) --> Rust type --> SqlValue
    /// ```
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - Value cannot be converted to `typ`.
    pub fn try_convert(&self, typ: &SqlType) -> Result<NnSqlValue> {
        match typ {
            SqlType::NumericComparable(n) => match n {
                NumericComparableType::I64Loose(i) => match i {
                    I64LooseType::SmallInt => self.unpack::<i16>().map(|v| v.into_nn_sql_value()),
                    I64LooseType::Integer => self.unpack::<i32>().map(|v| v.into_nn_sql_value()),
                    I64LooseType::BigInt => self.unpack::<i64>().map(|v| v.into_nn_sql_value()),
                },
                NumericComparableType::U64Loose(u) => match u {
                    U64LooseType::UnsignedInteger => {
                        self.unpack::<u32>().map(|v| v.into_nn_sql_value())
                    }
                    U64LooseType::UnsignedBigInt => {
                        self.unpack::<u64>().map(|v| v.into_nn_sql_value())
                    }
                },
                NumericComparableType::F32Loose(f) => match f {
                    F32LooseType::Float => self.unpack::<f32>().map(|v| v.into_nn_sql_value()),
                },
            },
            SqlType::StringComparableLoose(s) => match s {
                StringComparableLoseType::Text => {
                    self.unpack::<String>().map(|v| v.into_nn_sql_value())
                }
            },
            SqlType::BinaryComparable => self.unpack::<Vec<u8>>().map(|v| v.into_nn_sql_value()),
            SqlType::BooleanComparable => self.unpack::<bool>().map(|v| v.into_nn_sql_value()),
            SqlType::TimestampComparable => self
                .unpack::<SpringTimestamp>()
                .map(|v| v.into_nn_sql_value()),
            SqlType::DurationComparable => self
                .unpack::<SpringEventDuration>()
                .map(|v| v.into_nn_sql_value()),
        }
    }

    pub fn sql_compare(&self, other: &Self) -> Result<SqlCompareResult> {
        match (self.sql_type(), other.sql_type()) {
            (SqlType::NumericComparable(ref self_n), SqlType::NumericComparable(ref other_n)) => {
                match (self_n, other_n) {
                    (NumericComparableType::I64Loose(_), NumericComparableType::I64Loose(_)) => {
                        let (self_i64, other_i64) = (self.unpack::<i64>()?, other.unpack::<i64>()?);
                        Ok(SqlCompareResult::from(self_i64.cmp(&other_i64)))
                    }
                    (NumericComparableType::U64Loose(_), NumericComparableType::U64Loose(_)) => {
                        let (self_u64, other_u64) = (self.unpack::<u64>()?, other.unpack::<u64>()?);
                        Ok(SqlCompareResult::from(self_u64.cmp(&other_u64)))
                    }
                    (NumericComparableType::F32Loose(_), NumericComparableType::F32Loose(_)) => {
                        let (self_f32, other_f32) = (self.unpack::<f32>()?, other.unpack::<f32>()?);
                        Ok(SqlCompareResult::from(self_f32.partial_cmp(&other_f32)))
                    }
                    _ => Err(SpringError::Sql(anyhow!(
                        "Cannot compare {:?} and {:?}",
                        self_n,
                        other_n
                    ))),
                }
            }
            (SqlType::StringComparableLoose(self_s), SqlType::StringComparableLoose(other_s)) => {
                match (self_s, other_s) {
                    (StringComparableLoseType::Text, StringComparableLoseType::Text) => {
                        let (self_string, other_string) =
                            (self.unpack::<String>()?, other.unpack::<String>()?);
                        Ok(SqlCompareResult::from(self_string.cmp(&other_string)))
                    }
                }
            }
            (SqlType::BooleanComparable, SqlType::BooleanComparable) => {
                let (self_b, other_b) = (self.unpack::<bool>()?, other.unpack::<bool>()?);
                Ok(SqlCompareResult::from(self_b.cmp(&other_b)))
            }
            (SqlType::TimestampComparable, SqlType::TimestampComparable) => {
                let (self_t, other_t) = (
                    self.unpack::<SpringTimestamp>()?,
                    other.unpack::<SpringTimestamp>()?,
                );
                Ok(SqlCompareResult::from(self_t.cmp(&other_t)))
            }
            (_, _) => Err(SpringError::Sql(anyhow!(
                "`self` and `other` are not in comparable type - self: {:?}, other: {:?}",
                self,
                other
            ))),
        }
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - inner value cannot negate
    pub fn negate(self) -> Result<Self> {
        match self {
            NnSqlValue::SmallInt(v) => Ok(Self::SmallInt(-v)),
            NnSqlValue::Integer(v) => Ok(Self::Integer(-v)),
            NnSqlValue::BigInt(v) => Ok(Self::BigInt(-v)),
            NnSqlValue::Float(v) => Ok(Self::Float(-v)),

            NnSqlValue::UnsignedInteger(_)
            | NnSqlValue::UnsignedBigInt(_)
            | NnSqlValue::Text(_)
            | NnSqlValue::Blob(_)
            | NnSqlValue::Boolean(_)
            | NnSqlValue::Timestamp(_)
            | NnSqlValue::Duration(_) => Err(SpringError::Sql(anyhow!("{} cannot negate", self))),
        }
    }
}

impl From<NnSqlValue> for serde_json::Value {
    fn from(nn_sql_value: NnSqlValue) -> Self {
        match nn_sql_value {
            NnSqlValue::SmallInt(i) => serde_json::Value::from(i),
            NnSqlValue::Integer(i) => serde_json::Value::from(i),
            NnSqlValue::BigInt(i) => serde_json::Value::from(i),
            NnSqlValue::UnsignedInteger(u) => serde_json::Value::from(u),
            NnSqlValue::UnsignedBigInt(u) => serde_json::Value::from(u),
            NnSqlValue::Float(f) => serde_json::Value::from(f.into_inner()),
            NnSqlValue::Text(s) => serde_json::Value::from(s),
            NnSqlValue::Boolean(b) => serde_json::Value::from(b),
            NnSqlValue::Timestamp(t) => serde_json::Value::from(t.to_string()),
            NnSqlValue::Duration(_) => {
                unimplemented!("never appear in stream definition (just an intermediate type)")
            }
            NnSqlValue::Blob(_) => unimplemented!("cannot convert BLOB data into JSON"),
        }
    }
}

impl Add for NnSqlValue {
    type Output = Result<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self.sql_type(), rhs.sql_type()) {
            (SqlType::NumericComparable(ref self_n), SqlType::NumericComparable(ref rhs_n)) => {
                match (self_n, rhs_n) {
                    (NumericComparableType::I64Loose(_), NumericComparableType::I64Loose(_)) => {
                        let (self_i64, rhs_i64) = (self.unpack::<i64>()?, rhs.unpack::<i64>()?);
                        Ok(Self::BigInt(self_i64 + rhs_i64))
                    }
                    (NumericComparableType::U64Loose(_), NumericComparableType::U64Loose(_)) => {
                        let (self_u64, rhs_u64) = (self.unpack::<u64>()?, rhs.unpack::<u64>()?);
                        Ok(Self::UnsignedBigInt(self_u64 + rhs_u64))
                    }
                    (NumericComparableType::F32Loose(_), NumericComparableType::F32Loose(_)) => {
                        let (self_f32, rhs_f32) = (self.unpack::<f32>()?, rhs.unpack::<f32>()?);
                        Ok(Self::Float(OrderedFloat(self_f32 + rhs_f32)))
                    }
                    _ => Err(SpringError::Sql(anyhow!(
                        "Cannot add {:?} and {:?}",
                        self_n,
                        rhs_n
                    ))),
                }
            }
            (_, _) => Err(SpringError::Sql(anyhow!(
                "`self` + `rhs` is undefined - self: {:?}, other: {:?}",
                self,
                rhs
            ))),
        }
    }
}
impl Mul for NnSqlValue {
    type Output = Result<Self>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self.sql_type(), rhs.sql_type()) {
            (SqlType::NumericComparable(ref self_n), SqlType::NumericComparable(ref rhs_n)) => {
                match (self_n, rhs_n) {
                    (NumericComparableType::I64Loose(_), NumericComparableType::I64Loose(_)) => {
                        let (self_i64, rhs_i64) = (self.unpack::<i64>()?, rhs.unpack::<i64>()?);
                        Ok(Self::BigInt(self_i64 * rhs_i64))
                    }
                    (NumericComparableType::U64Loose(_), NumericComparableType::U64Loose(_)) => {
                        let (self_u64, rhs_u64) = (self.unpack::<u64>()?, rhs.unpack::<u64>()?);
                        Ok(Self::UnsignedBigInt(self_u64 * rhs_u64))
                    }
                    (NumericComparableType::F32Loose(_), NumericComparableType::F32Loose(_)) => {
                        let (self_f32, rhs_f32) = (self.unpack::<f32>()?, rhs.unpack::<f32>()?);
                        Ok(Self::Float(OrderedFloat(self_f32 * rhs_f32)))
                    }
                    _ => Err(SpringError::Sql(anyhow!(
                        "Cannot multiply {:?} by {:?}",
                        self_n,
                        rhs_n
                    ))),
                }
            }
            (_, _) => Err(SpringError::Sql(anyhow!(
                "`self` + `rhs` is undefined - self: {:?}, other: {:?}",
                self,
                rhs
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unpack_loosely() -> Result<()> {
        assert_eq!(NnSqlValue::SmallInt(-1).unpack::<i16>()?, -1);
        assert_eq!(NnSqlValue::SmallInt(-1).unpack::<i32>()?, -1);
        assert_eq!(NnSqlValue::SmallInt(-1).unpack::<i64>()?, -1);

        assert_eq!(NnSqlValue::Integer(-1).unpack::<i16>()?, -1);
        assert_eq!(NnSqlValue::Integer(-1).unpack::<i32>()?, -1);
        assert_eq!(NnSqlValue::Integer(-1).unpack::<i64>()?, -1);

        assert_eq!(NnSqlValue::BigInt(-1).unpack::<i16>()?, -1);
        assert_eq!(NnSqlValue::BigInt(-1).unpack::<i32>()?, -1);
        assert_eq!(NnSqlValue::BigInt(-1).unpack::<i64>()?, -1);

        assert_eq!(
            NnSqlValue::UnsignedInteger(u16::MAX as u32).unpack::<u32>()?,
            u16::MAX as u32
        );
        assert_eq!(
            NnSqlValue::UnsignedInteger(u16::MAX as u32).unpack::<u64>()?,
            u16::MAX as u64
        );

        assert_eq!(
            NnSqlValue::UnsignedBigInt(u16::MAX as u64).unpack::<u64>()?,
            u16::MAX as u64
        );

        assert_eq!(
            NnSqlValue::Text("🚔".to_string()).unpack::<String>()?,
            "🚔".to_string()
        );

        assert!(NnSqlValue::Boolean(true).unpack::<bool>()?);
        assert!(!NnSqlValue::Boolean(false).unpack::<bool>()?);

        assert_eq!(
            NnSqlValue::Timestamp(SpringTimestamp::fx_ts1()).unpack::<SpringTimestamp>()?,
            SpringTimestamp::fx_ts1()
        );

        Ok(())
    }

    #[test]
    fn test_unpack_blob() {
        assert_eq!(
            NnSqlValue::Blob(b"hello".to_vec())
                .unpack::<Vec<u8>>()
                .unwrap(),
            b"hello".to_vec()
        );
    }
}
