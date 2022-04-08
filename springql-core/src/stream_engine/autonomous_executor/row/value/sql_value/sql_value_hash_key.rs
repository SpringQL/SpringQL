// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use super::SqlValue;

/// `Eq + Hash` hash key used for hash algorithms.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SqlValueHashKey(u64);

impl From<&SqlValue> for SqlValueHashKey {
    fn from(sql_value: &SqlValue) -> Self {
        let mut hasher = DefaultHasher::new();
        sql_value.hash(&mut hasher);
        Self(hasher.finish())
    }
}
