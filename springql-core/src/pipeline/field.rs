// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! A field is a part of a tuple.
//! Types of fields are:
//!
//! - attribute in correlation (denoted as `correlation.attribute`).
//! - constant (e.g. `777`, `"abc").
//! - other expressions (e.g. `c1 + 1 AS c1p`).

mod field_name;

pub use field_name::ColumnReference;

use crate::{mem_size::MemSize, stream_engine::SqlValue};

/// Field == SqlValue + ColumnReference
#[derive(Clone, PartialEq, Debug, new)]
pub struct Field {
    name: ColumnReference,
    value: SqlValue,
}

impl MemSize for Field {
    fn mem_size(&self) -> usize {
        self.name.mem_size() + self.value.mem_size()
    }
}

impl Field {
    pub fn name(&self) -> &ColumnReference {
        &self.name
    }

    pub fn sql_value(&self) -> &SqlValue {
        &self.value
    }
}
