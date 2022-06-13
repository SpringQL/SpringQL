// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    mem_size::MemSize,
    pipeline::name::{ColumnName, StreamName},
};

/// Reference to a column in a row.
///
/// Note that this never point to other expressions like `1 + 1 AS a`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub struct ColumnReference {
    pub stream_name: StreamName,
    pub column_name: ColumnName,
}

impl MemSize for ColumnReference {
    fn mem_size(&self) -> usize {
        self.stream_name.mem_size() + self.column_name.mem_size()
    }
}
