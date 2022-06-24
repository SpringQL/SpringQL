// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::Result,
    stream_engine::autonomous_executor::row::{
        foreign_row::format::JsonObject, schemaless_row::SchemalessRow,
    },
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct JsonSourceRow(JsonObject);

impl JsonSourceRow {
    pub fn parse(json_s: &str) -> Result<Self> {
        let json_obj = JsonObject::parse(json_s)?;
        Ok(Self::from_json(json_obj))
    }

    pub fn from_json(json: JsonObject) -> Self {
        Self(json)
    }

    pub fn into_schemaless_row(self) -> Result<SchemalessRow> {
        // JsonSourceRow -> JsonObject -> ColumnValues -> SchemalessRow
        let column_values = self.0.into_column_values()?;
        Ok(column_values.into())
    }
}
