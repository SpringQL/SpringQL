// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::{autonomous_executor::ColumnValues, StreamRow};

/// Row untied with a stream.
/// Used for foreign sources and sinks.
///
/// - PartialEq by all columns (NULL prevents Eq).
#[derive(Clone, PartialEq, Debug)]
pub struct SchemalessRow {
    /// Columns
    colvals: ColumnValues,
}

impl SchemalessRow {
    pub fn into_column_values(self) -> ColumnValues {
        self.colvals
    }
}

impl From<StreamRow> for SchemalessRow {
    fn from(stream_row: StreamRow) -> Self {
        let colvals = stream_row.into();
        Self { colvals }
    }
}

impl From<ColumnValues> for SchemalessRow {
    fn from(colvals: ColumnValues) -> Self {
        Self { colvals }
    }
}
