use std::sync::Arc;

use crate::{
    error::Result,
    pipeline::stream_model::stream_shape::StreamShape,
    stream_engine::{
        autonomous_executor::row::{column::stream_column::StreamColumns, Row},
        dependency_injection::DependencyInjection,
    },
};

use super::format::json::JsonObject;

/// Input row from foreign sources (retrieved from SourceServer).
///
/// Immediately converted into Row on stream-engine boundary.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine) struct ForeignSourceRow(JsonObject);

impl ForeignSourceRow {
    pub(in crate::stream_engine) fn from_json(json: JsonObject) -> Self {
        Self(json)
    }

    /// # Failure
    ///
    /// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
    ///   - This foreign input row cannot be converted into row.
    pub(in crate::stream_engine::autonomous_executor) fn into_row<DI: DependencyInjection>(
        self,
        stream_shape: Arc<StreamShape>,
    ) -> Result<Row> {
        // ForeignSourceRow -> JsonObject -> HashMap<ColumnName, SqlValue> -> StreamColumns -> Row

        let column_values = self.0.into_column_values()?;
        let stream_columns = StreamColumns::new(stream_shape, column_values)?;
        Ok(Row::new::<DI>(stream_columns))
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_engine::dependency_injection::test_di::TestDI;

    use super::*;

    #[test]
    fn test_json_into_row() {
        let stream = Arc::new(StreamShape::fx_city_temperature());

        let fr = ForeignSourceRow::fx_city_temperature_tokyo();
        let r = Row::fx_city_temperature_tokyo();
        assert_eq!(fr.into_row::<TestDI>(stream).unwrap(), r);
    }
}
