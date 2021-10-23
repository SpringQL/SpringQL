use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Comparison result of two [SqlValue](crate::SqlValue)s.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum SqlCompareResult {
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
impl SqlCompareResult {
    /// Whether self is Self::Eq
    pub fn is_equal(&self) -> bool {
        matches!(self, SqlCompareResult::Eq)
    }
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
