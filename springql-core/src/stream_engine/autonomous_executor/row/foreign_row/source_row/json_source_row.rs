// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{
    api::error::Result,
    pipeline::StreamModel,
    stream_engine::autonomous_executor::row::{
        column::StreamColumns, foreign_row::format::JsonObject, StreamRow,
    },
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
#[derive(Eq, PartialEq, Debug)]
pub struct JsonSourceRow(JsonObject);

impl JsonSourceRow {
    pub fn parse(json_s: &str) -> Result<Self> {
        let json_obj = JsonObject::parse(json_s)?;
        Ok(Self::from_json(json_obj))
    }

    pub fn from_json(json: JsonObject) -> Self {
        Self(json)
    }

    pub fn into_row(self, stream_model: Arc<StreamModel>) -> Result<StreamRow> {
        // SourceRow -> JsonObject -> HashMap<ColumnName, SqlValue> -> StreamColumns -> Row

        let column_values = self.0.into_column_values()?;
        let stream_columns = StreamColumns::new(stream_model, column_values)?;
        Ok(StreamRow::new(stream_columns))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_json_into_row() {
        let stream = Arc::new(StreamModel::fx_city_temperature());

        let fr = JsonSourceRow::fx_city_temperature_tokyo();
        let r = StreamRow::fx_city_temperature_tokyo();
        assert_eq!(fr.into_row(stream).unwrap(), r);
    }
}
