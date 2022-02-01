// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::mem::size_of;
use std::{fmt::Display, hash::Hash};

use super::sql_compare_result::SqlCompareResult;
use crate::error::{Result, SpringError};
use crate::mem_size::MemSize;
use crate::pipeline::relation::sql_type::{
    self, NumericComparableType, SqlType, StringComparableLoseType,
};
use crate::stream_engine::autonomous_executor::row::value::sql_convertible::SqlConvertible;
use crate::stream_engine::time::duration::event_duration::EventDuration;
use crate::stream_engine::time::timestamp::Timestamp;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};

/// NOT NULL value.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum NnSqlValue {
    /// SMALLINT
    SmallInt(i16),
    /// INTEGER
    Integer(i32),
    /// BIGINT
    BigInt(i64),

    /// TEXT
    Text(String),

    /// BOOLEAN
    Boolean(bool),

    /// TIMESTAMP
    Timestamp(Timestamp),

    /// DURATION
    Duration(EventDuration),
}

impl MemSize for NnSqlValue {
    fn mem_size(&self) -> usize {
        match self {
            NnSqlValue::SmallInt(_) => size_of::<i16>(),
            NnSqlValue::Integer(_) => size_of::<i32>(),
            NnSqlValue::BigInt(_) => size_of::<i64>(),

            NnSqlValue::Text(s) => s.capacity(),

            NnSqlValue::Boolean(_) => size_of::<bool>(),

            NnSqlValue::Timestamp(ts) => ts.mem_size(),

            NnSqlValue::Duration(dur) => dur.mem_size(),
        }
    }
}

/// Although function is better to use,
///
/// ```
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
    ( $nn_sql_value:expr, $closure_i64:expr, $closure_string:expr, $closure_bool:expr, $closure_timestamp:expr, $closure_duration:expr ) => {{
        match &$nn_sql_value {
            NnSqlValue::SmallInt(_) | NnSqlValue::Integer(_) | NnSqlValue::BigInt(_) => {
                let v = $nn_sql_value.unpack::<i64>().unwrap();
                $closure_i64(v)
            }
            NnSqlValue::Text(s) => $closure_string(s.to_string()),
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
            |s: String| {
                s.hash(state);
            },
            |b: bool| { b.hash(state) },
            |t: Timestamp| { t.hash(state) },
            |d: EventDuration| { d.hash(state) }
        )
    }
}

impl Display for NnSqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = for_all_loose_types!(
            self,
            |i: i64| i.to_string(),
            |s: String| format!(r#""{}""#, s),
            |b: bool| (if b { "TRUE" } else { "FALSE" }).to_string(),
            |t: Timestamp| t.to_string(),
            |d: EventDuration| d.to_string()
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
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Any value of `T` cannot be typed as this SqlValue's SqlType (E.g. `T = i64`, `SqlType = SmallInt`).
    pub fn unpack<T>(&self) -> Result<T>
    where
        T: SqlConvertible,
    {
        match self {
            NnSqlValue::SmallInt(i16_) => T::try_from_i16(i16_),
            NnSqlValue::Integer(i32_) => T::try_from_i32(i32_),
            NnSqlValue::BigInt(i64_) => T::try_from_i64(i64_),
            NnSqlValue::Text(string) => T::try_from_string(string),
            NnSqlValue::Boolean(b) => T::try_from_bool(b),
            NnSqlValue::Timestamp(t) => T::try_from_timestamp(t),
            NnSqlValue::Duration(d) => T::try_from_duration(d),
        }
    }

    /// SqlType of this value
    pub(crate) fn sql_type(&self) -> SqlType {
        match self {
            NnSqlValue::SmallInt(_) => SqlType::small_int(),
            NnSqlValue::Integer(_) => SqlType::integer(),
            NnSqlValue::BigInt(_) => SqlType::big_int(),
            NnSqlValue::Text(_) => SqlType::text(),
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
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Value cannot be converted to `typ`.
    pub(crate) fn try_convert(&self, typ: &SqlType) -> Result<NnSqlValue> {
        match typ {
            SqlType::NumericComparable(n) => match n {
                NumericComparableType::I64Loose(i) => match i {
                    sql_type::I64LooseType::SmallInt => {
                        self.unpack::<i16>().map(|v| v.into_sql_value())
                    }
                    sql_type::I64LooseType::Integer => {
                        self.unpack::<i32>().map(|v| v.into_sql_value())
                    }
                    sql_type::I64LooseType::BigInt => {
                        self.unpack::<i64>().map(|v| v.into_sql_value())
                    }
                },
            },
            SqlType::StringComparableLoose(s) => match s {
                StringComparableLoseType::Text => {
                    self.unpack::<String>().map(|v| v.into_sql_value())
                }
            },
            SqlType::BooleanComparable => self.unpack::<bool>().map(|v| v.into_sql_value()),
            SqlType::TimestampComparable => self.unpack::<Timestamp>().map(|v| v.into_sql_value()),
            SqlType::DurationComparable => {
                self.unpack::<EventDuration>().map(|v| v.into_sql_value())
            }
        }
    }

    pub(super) fn sql_compare(&self, other: &Self) -> Result<SqlCompareResult> {
        match (self.sql_type(), other.sql_type()) {
            (SqlType::NumericComparable(self_n), SqlType::NumericComparable(other_n)) => {
                match (self_n, other_n) {
                    (NumericComparableType::I64Loose(_), NumericComparableType::I64Loose(_)) => {
                        let (self_i64, other_i64) = (self.unpack::<i64>()?, other.unpack::<i64>()?);
                        Ok(SqlCompareResult::from(self_i64.cmp(&other_i64)))
                    }
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
                let (self_t, other_t) = (self.unpack::<Timestamp>()?, other.unpack::<Timestamp>()?);
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
    pub(crate) fn negate(self) -> Result<Self> {
        match self {
            NnSqlValue::SmallInt(v) => Ok(Self::SmallInt(-v)),
            NnSqlValue::Integer(v) => Ok(Self::Integer(-v)),
            NnSqlValue::BigInt(v) => Ok(Self::BigInt(-v)),
            NnSqlValue::Text(_)
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
            NnSqlValue::Text(s) => serde_json::Value::from(s),
            NnSqlValue::Boolean(b) => serde_json::Value::from(b),
            NnSqlValue::Timestamp(t) => serde_json::Value::from(t.to_string()),
            NnSqlValue::Duration(_) => {
                unimplemented!("never appear in stream definition (just an intermediate type)")
            }
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
            NnSqlValue::Text("🚔".to_string()).unpack::<String>()?,
            "🚔".to_string()
        );

        assert!(NnSqlValue::Boolean(true).unpack::<bool>()?);
        assert!(!NnSqlValue::Boolean(false).unpack::<bool>()?);

        assert_eq!(
            NnSqlValue::Timestamp(Timestamp::fx_ts1()).unpack::<Timestamp>()?,
            Timestamp::fx_ts1()
        );

        Ok(())
    }
}
