// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    mem_size::MemSize,
    pipeline::name::{ColumnName, StreamName},
};

/// Reference to a column in a row.
///
/// Note that this never point to other expressions like `1 + 1 AS a`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum ColumnReference {
    /// Normal column reference
    Column {
        stream_name: StreamName,
        column_name: ColumnName,
    },
    /// Processing time
    PTime { stream_name: StreamName },
}

impl MemSize for ColumnReference {
    fn mem_size(&self) -> usize {
        match self {
            Self::Column {
                stream_name,
                column_name,
            } => stream_name.mem_size() + column_name.mem_size(),
            Self::PTime { stream_name } => stream_name.mem_size(),
        }
    }
}
