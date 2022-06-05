// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

/// SQL type.
///
/// SQL types are hierarchically categorized as follows:
///
/// - Comparable types: two values are **comparable** (, and some types are also **ordered**).
///   - Loose types: values can be typed as 1 specific Rust type.
///     - SQL types: corresponds to an SQL type.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum SqlType {
    /// Numeric types
    NumericComparable(NumericComparableType),

    /// String types
    StringComparableLoose(StringComparableLoseType),

    /// Binary types
    BinaryComparable,

    /// Boolean types
    BooleanComparable,

    /// Timestamp types
    TimestampComparable,

    /// Duration types
    DurationComparable,
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

    /// Constructor of Float
    pub fn float() -> SqlType {
        SqlType::NumericComparable(NumericComparableType::F32Loose(F32LooseType::Float))
    }

    /// Constructor of Text
    pub fn text() -> SqlType {
        SqlType::StringComparableLoose(StringComparableLoseType::Text)
    }

    /// Constructor of Blob
    pub fn blob() -> SqlType {
        SqlType::BinaryComparable
    }

    /// Constructor of Boolean
    pub fn boolean() -> SqlType {
        SqlType::BooleanComparable
    }

    /// Constructor of Timestamp
    pub fn timestamp() -> SqlType {
        SqlType::TimestampComparable
    }

    /// Constructor of Duration
    pub fn duration() -> SqlType {
        SqlType::DurationComparable
    }
}

/// Numeric types (comparable).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum NumericComparableType {
    /// Loosely typed as i64
    I64Loose(I64LooseType),

    /// Loosely typed as f32
    F32Loose(F32LooseType),
}

/// Integer types (loosely typed as i64).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum I64LooseType {
    /// 2-byte signed integer.
    SmallInt,

    /// 4-byte signed integer.
    Integer,

    /// 8-byte signed integer.
    BigInt,
}

/// Float types (loosely typed as f64).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum F32LooseType {
    /// fp32
    Float,
}

/// Text types (comparable, loosely typed as String).
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum StringComparableLoseType {
    /// Arbitrary length text (UTF-8).
    Text,
}
