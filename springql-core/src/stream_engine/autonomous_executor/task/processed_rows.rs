// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::ops::Add;

/// Rows processed per execution
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, new)]
pub struct ProcessedRows(u64);

impl Add for ProcessedRows {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl ProcessedRows {
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}
