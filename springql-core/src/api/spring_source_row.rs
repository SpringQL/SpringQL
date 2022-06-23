// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::Result,
    stream_engine::autonomous_executor::{SchemalessRow, SourceRow},
};

/// Row object from an in memory sink queue.
#[derive(Clone, Debug)]
pub struct SpringSourceRow(SourceRow);

impl SpringSourceRow {
    pub(crate) fn new(source_row: SchemalessRow) -> Self {
        let source_row = SourceRow::Raw(source_row);
        Self(source_row)
    }

    pub(crate) fn into_schemaless_row(self) -> Result<SchemalessRow> {
        self.0.try_into()
    }
}
