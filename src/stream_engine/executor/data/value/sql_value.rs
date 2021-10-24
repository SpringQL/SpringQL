pub(in crate::stream_engine::executor) mod nn_sql_value;
pub(in crate::stream_engine::executor) mod sql_compare_result;
pub(in crate::stream_engine::executor) mod sql_value_hash_key;

use self::{nn_sql_value::NnSqlValue, sql_compare_result::SqlCompareResult};
use crate::error::{Result, SpringError};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, hash::Hash};

/// SQL-typed value that is efficiently compressed.
///
/// # Hiding Rust type inside
///
/// It is important feature for SqlValue not to take any type parameter (although some associated methods do).
/// If SqlValue takes any type parameter, collection types holding SqlType have to use impl/dyn trait.
///
/// # Comparing SqlValues
///
/// An SqlValue implements is NULL or NOT NULL.
/// NOT NULL value has its SQL type in [SqlType](crate::SqlType).
/// SqlType forms hierarchical structure and if its comparable top-level variant (e.g. [SqlType::NumericComparable](crate::SqlType::NumericComparable)) are the same among two values,
/// these two are **comparable**, meaning equality comparison to them is valid.
/// Also, ordered comparison is valid for values within some top-level variant of Constant.
/// Such variants and values within one are called **ordered**.
/// **Ordered** is stronger property than **comparable**.
///
/// ## Failures on comparison
///
/// Comparing non-**comparable** values and ordered comparison to non-**ordered** values cause [SqlState::DataExceptionIllegalComparison](crate::SqlState::DataExceptionIllegalComparison).
///
/// ## Comparison with NULL
///
/// Any SqlValue can calculate equality- and ordered- comparison with NULL value.
///
/// Equality-comparison and ordered-comparison with NULL is evaluated to NULL.
/// NULL is always evaluated as FALSE in boolean context (, therefore all of `x = NULL`, `x != NULL`, `x < NULL`, `x > NULL` are evaluated to FALSE in boolean context).
///
/// # Hashing SqlValues
///
/// Hashed values are sometimes used in query execution (e.g. hash-join, hash-aggregation).
/// SqlValue implements `Hash` but does not `Eq` so SqlValue cannot be used as hash key of `HashMap` and `HashSet`.
///
/// Use [SqlValueHashKey](self::sql_value_hash_key::SqlValueHashKey) for that purpose.
///
/// # Examples
///
/// See: [test_sql_value_example()](self::tests::test_sql_value_example).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) enum SqlValue {
    /// NULL value.
    Null,
    /// NOT NULL value.
    NotNull(NnSqlValue),
}

impl PartialEq for SqlValue {
    fn eq(&self, other: &Self) -> bool {
        matches!(self.sql_compare(other), Ok(SqlCompareResult::Eq))
    }
}

impl Hash for SqlValue {
    /// Generates different hash value for each NULL value to avoid collision in hash table.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            SqlValue::Null => {
                let v = fastrand::u64(..);
                v.hash(state);
            }
            SqlValue::NotNull(nn_sql_value) => nn_sql_value.hash(state),
        }
    }
}

impl Display for SqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SqlValue::Null => "NULL".to_string(),
            SqlValue::NotNull(nn) => nn.to_string(),
        };
        write!(f, "{}", s)
    }
}

impl SqlValue {
    /// Compares two SqlValues.
    ///
    /// # Failures
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `self` and `other` have different top-level variant of [SqlType](crate::SqlType).
    ///
    /// # Examples
    ///
    /// See: [test_sql_compare_example()](self::tests::test_sql_compare_example).
    pub fn sql_compare(&self, other: &Self) -> Result<SqlCompareResult> {
        match (self, other) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlCompareResult::Null),
            (SqlValue::NotNull(nn_self), SqlValue::NotNull(nn_other)) => {
                nn_self.sql_compare(nn_other)
            }
        }
    }

    /// Eval as bool if possible.
    ///
    /// # Failures
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - inner value cannot negate
    pub fn _to_bool(&self) -> Result<bool> {
        match self {
            SqlValue::Null => Ok(false), // NULL is always evaluated as FALSE
            SqlValue::NotNull(nn_sql_value) => match nn_sql_value {
                NnSqlValue::Boolean(b) => Ok(*b),
                _ => Err(SpringError::Sql(anyhow!(
                    "{:?} cannot be evaluated as BOOLEAN",
                    nn_sql_value.sql_type()
                ))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::stream_engine::executor::data::value::sql_value::sql_value_hash_key::SqlValueHashKey;

    use super::*;

    #[allow(clippy::eq_op)]
    #[test]
    fn test_sql_value_example() {
        let v_integer = SqlValue::NotNull(NnSqlValue::Integer(42));
        let v_smallint = SqlValue::NotNull(NnSqlValue::SmallInt(42));
        let v_bigint = SqlValue::NotNull(NnSqlValue::BigInt(42));
        let v_null = SqlValue::Null;

        assert_eq!(v_integer, v_integer);
        assert_eq!(
            v_smallint, v_bigint,
            "Comparing SmallInt with BigInt is valid"
        );
        assert_ne!(v_null, v_null, "NULL != NULL");

        let mut hash_set = HashSet::<SqlValueHashKey>::new();
        assert!(hash_set.insert(SqlValueHashKey::from(&v_integer)));
        assert!(
            !hash_set.insert(SqlValueHashKey::from(&v_integer)),
            "same value is already inserted"
        );
        assert!(
            !hash_set.insert(SqlValueHashKey::from(&v_smallint)),
            "same hash values are generated from both SmallInt and Integer"
        );
        assert!(hash_set.insert(SqlValueHashKey::from(&v_null)),);
        assert!(
            hash_set.insert(SqlValueHashKey::from(&v_null)),
            "two NULL values are different"
        );

        assert_ne!(
            SqlValueHashKey::from(&v_null),
            SqlValueHashKey::from(&v_null),
            "two NULL values generates different Hash value"
        );
    }

    #[test]
    fn test_sql_compare_example() -> Result<()> {
        let v_integer = SqlValue::NotNull(NnSqlValue::Integer(42));
        let v_smallint = SqlValue::NotNull(NnSqlValue::SmallInt(42));
        let v_bigint = SqlValue::NotNull(NnSqlValue::BigInt(42));
        let v_integer_minus = SqlValue::NotNull(NnSqlValue::Integer(-42));
        let v_text = SqlValue::NotNull(NnSqlValue::Text("abc".to_string()));
        let v_null = SqlValue::Null;

        assert!(matches!(
            v_integer.sql_compare(&v_integer)?,
            SqlCompareResult::Eq
        ));
        assert!(matches!(
            v_smallint.sql_compare(&v_bigint)?,
            SqlCompareResult::Eq
        ));
        assert!(matches!(
            v_integer.sql_compare(&v_integer_minus)?,
            SqlCompareResult::GreaterThan
        ));
        assert!(matches!(
            v_integer_minus.sql_compare(&v_integer)?,
            SqlCompareResult::LessThan
        ));
        assert!(matches!(
            v_null.sql_compare(&v_integer)?,
            SqlCompareResult::Null
        ));
        assert!(matches!(
            v_integer.sql_compare(&v_null)?,
            SqlCompareResult::Null
        ));
        assert!(matches!(
            v_null.sql_compare(&v_null)?,
            SqlCompareResult::Null
        ));

        assert!(matches!(
            v_integer
                .sql_compare(&v_text)
                .expect_err("comparing totally different types"),
            SpringError::Sql(_),
        ));

        Ok(())
    }
}
