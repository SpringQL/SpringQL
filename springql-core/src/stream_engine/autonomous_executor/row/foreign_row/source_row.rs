// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{
    api::error::Result,
    pipeline::stream_model::StreamModel,
    stream_engine::autonomous_executor::row::{
        column::stream_column::StreamColumns, foreign_row::format::json::JsonObject, Row,
    },
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into Row on stream-engine boundary.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine) struct SourceRow(JsonObject);

impl SourceRow {
    pub(in crate::stream_engine) fn from_json(json: JsonObject) -> Self {
        Self(json)
    }

    /// # Failure
    ///
    /// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
    ///   - This input row cannot be converted into row.
    pub(in crate::stream_engine::autonomous_executor) fn into_row(
        self,
        stream_model: Arc<StreamModel>,
    ) -> Result<Row> {
        // SourceRow -> JsonObject -> HashMap<ColumnName, SqlValue> -> StreamColumns -> Row

        let column_values = self.0.into_column_values()?;
        let stream_columns = StreamColumns::new(stream_model, column_values)?;
        Ok(Row::new(stream_columns))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_json_into_row() {
        let stream = Arc::new(StreamModel::fx_city_temperature());

        let fr = SourceRow::fx_city_temperature_tokyo();
        let r = Row::fx_city_temperature_tokyo();
        assert_eq!(fr.into_row(stream).unwrap(), r);
    }
}
