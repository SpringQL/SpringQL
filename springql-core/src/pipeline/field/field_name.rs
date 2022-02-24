// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    mem_size::MemSize,
    pipeline::name::{ColumnName, StreamName},
};

/// Reference to a column in a row.
///
/// Note that this never point to other expressions like `1 + 1 AS a`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct ColumnReference {
    pub(crate) stream_name: StreamName,
    pub(crate) column_name: ColumnName,
}

impl MemSize for ColumnReference {
    fn mem_size(&self) -> usize {
        self.stream_name.mem_size() + self.column_name.mem_size()
    }
}
