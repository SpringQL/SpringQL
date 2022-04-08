// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::cmp::Ordering;

/// Comparison result of two [SqlValue](crate::SqlValue)s.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum SqlCompareResult {
    /// v1 = v2
    Eq,

    /// v1 < v2.
    /// Only applicable for ordered values.
    LessThan,

    /// v1 > v2.
    /// Only applicable for ordered values.
    GreaterThan,

    /// v1 != v2.
    /// Only applicable for non-ordered values.
    NotEq,

    /// Either of v1 or v2 is NULL.
    Null,
}

impl From<Ordering> for SqlCompareResult {
    fn from(ord: Ordering) -> Self {
        match ord {
            Ordering::Less => Self::LessThan,
            Ordering::Equal => Self::Eq,
            Ordering::Greater => Self::GreaterThan,
        }
    }
}

/// for partial_cmp
impl From<Option<Ordering>> for SqlCompareResult {
    fn from(partial_ord: Option<Ordering>) -> Self {
        match partial_ord {
            Some(Ordering::Less) => Self::LessThan,
            Some(Ordering::Equal) => Self::Eq,
            Some(Ordering::Greater) => Self::GreaterThan,
            None => Self::Null,
        }
    }
}
