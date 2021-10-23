use serde::{Deserialize, Serialize};

/// SQL type.
///
/// SQL types are hierarchically categorized as follows:
///
/// - Comparable types: two values are **comparable** (, and some types are also **ordered**).
///   - Loose types: values can be typed as 1 specific Rust type.
///     - SQL types: corresponds to an SQL type.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum SqlType {
    /// Numeric types
    NumericComparable(NumericComparableType),

    /// String types
    StringComparableLoose(StringComparableLoseType),

    /// Boolean types
    BooleanComparable,
}

impl SqlType {
    /// Constructor of SmallInt
    pub fn small_int() -> SqlType {
        SqlType::NumericComparable(NumericComparableType::I64Loose(I64LooseType::SmallInt))
    }
    /// Constructor of Integer
    pub fn integer() -> SqlType {
        SqlType::NumericComparable(NumericComparableType::I64Loose(I64LooseType::Integer))
    }
    /// Constructor of BigInt
    pub fn big_int() -> SqlType {
        SqlType::NumericComparable(NumericComparableType::I64Loose(I64LooseType::BigInt))
    }

    /// Constructor of Text
    pub fn text() -> SqlType {
        SqlType::StringComparableLoose(StringComparableLoseType::Text)
    }

    /// Constructor of Boolean
    pub fn boolean() -> SqlType {
        SqlType::BooleanComparable
    }
}

/// Numeric types (comparable).
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum NumericComparableType {
    /// Loosely typed as i64
    I64Loose(I64LooseType),
}

/// Integer types (loosely typed as i64).
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum I64LooseType {
    /// 2-byte signed integer.
    SmallInt,

    /// 4-byte signed integer.
    Integer,

    /// 8-byte signed integer.
    BigInt,
}

/// Text types (comparable, loosely typed as String).
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum StringComparableLoseType {
    /// Arbitrary length text (UTF-8).
    Text,
}
