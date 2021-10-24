use std::{fmt::Display, hash::Hash};

use super::sql_compare_result::SqlCompareResult;
use crate::error::{Result, SpringError};
use crate::model::sql_type::{NumericComparableType, SqlType, StringComparableLoseType};
use crate::stream_engine::executor::data::value::sql_convertible::SqlConvertible;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};

/// NOT NULL value.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NnSqlValue {
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
    ( $nn_sql_value:expr, $closure_i64:expr, $closure_string:expr, $closure_bool:expr ) => {{
        match &$nn_sql_value {
            NnSqlValue::SmallInt(_) | NnSqlValue::Integer(_) | NnSqlValue::BigInt(_) => {
                let v = $nn_sql_value.unpack::<i64>().unwrap();
                $closure_i64(v)
            }
            NnSqlValue::Text(s) => $closure_string(s.to_string()),
            NnSqlValue::Boolean(b) => $closure_bool(b.clone()),
        }
    }};
}

impl PartialEq for NnSqlValue {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.sql_compare(other), Ok(SqlCompareResult::Eq))
    }
}

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
            |b: bool| { b.hash(state) }
        )
    }
}

impl Display for NnSqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: String = for_all_loose_types!(
            self,
            |i: i64| i.to_string(),
            |s: String| format!(r#""{}""#, s),
            |b: bool| (if b { "TRUE" } else { "FALSE" }).to_string()
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
        }
    }

    /// SqlType of this value
    pub fn sql_type(&self) -> SqlType {
        match self {
            NnSqlValue::SmallInt(_) => SqlType::small_int(),
            NnSqlValue::Integer(_) => SqlType::integer(),
            NnSqlValue::BigInt(_) => SqlType::big_int(),
            NnSqlValue::Text(_) => SqlType::text(),
            NnSqlValue::Boolean(_) => SqlType::boolean(),
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
            (_, _) => Err(SpringError::Sql(anyhow!(
                "`self` and `other` are not in comparable type - self: {:?}, other: {:?}",
                self,
                other
            ))),
        }
    }

    /// # Failures
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - inner value cannot negate
    pub(crate) fn _negate(self) -> Result<Self> {
        match self {
            NnSqlValue::SmallInt(v) => Ok(Self::SmallInt(-v)),
            NnSqlValue::Integer(v) => Ok(Self::Integer(-v)),
            NnSqlValue::BigInt(v) => Ok(Self::BigInt(-v)),
            NnSqlValue::Text(_) | NnSqlValue::Boolean(_) => {
                Err(SpringError::Sql(anyhow!("{} cannot negate", self)))
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

        assert!(matches!(
            NnSqlValue::Integer(-1).unpack::<i16>().unwrap_err(),
            SpringError::Sql(_),
        ));
        assert_eq!(NnSqlValue::Integer(-1).unpack::<i32>()?, -1);
        assert_eq!(NnSqlValue::Integer(-1).unpack::<i64>()?, -1);

        assert!(matches!(
            NnSqlValue::BigInt(-1).unpack::<i16>().unwrap_err(),
            SpringError::Sql(_),
        ));
        assert!(matches!(
            NnSqlValue::BigInt(-1).unpack::<i32>().unwrap_err(),
            SpringError::Sql(_),
        ));
        assert_eq!(NnSqlValue::BigInt(-1).unpack::<i64>()?, -1);

        assert_eq!(
            NnSqlValue::Text("🚔".to_string()).unpack::<String>()?,
            "🚔".to_string()
        );

        Ok(())
    }
}
