use std::rc::Rc;

use crate::{
    dependency_injection::DependencyInjection,
    error::Result,
    model::pipeline::stream_model::stream_shape::StreamShape,
    stream_engine::executor::data::{column::stream_column::StreamColumns, row::Row},
};

use super::format::json::JsonObject;

/// Input row from foreign systems (retrieved from InputServer).
///
/// Immediately converted into Row on stream-engine boundary.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct ForeignInputRow(JsonObject);

impl ForeignInputRow {
    pub(in crate::stream_engine::executor) fn from_json(json: JsonObject) -> Self {
        Self(json)
    }

    /// # Failure
    ///
    /// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
    ///   - This foreign input row cannot be converted into row.
    pub(in crate::stream_engine::executor) fn into_row<DI: DependencyInjection>(
        self,
        stream_shape: Rc<StreamShape>,
    ) -> Result<Row> {
        // ForeignInputRow -> JsonObject -> HashMap<ColumnName, SqlValue> -> StreamColumns -> Row

        let column_values = self.0.into_column_values()?;
        let stream_columns = StreamColumns::new(stream_shape, column_values)?;
        Ok(Row::new::<DI>(stream_columns))
    }
}

#[cfg(test)]
mod tests {
    use crate::dependency_injection::test_di::TestDI;

    use super::*;

    #[test]
    fn test_json_into_row() {
        let stream = Rc::new(StreamShape::fx_city_temperature());

        let fr = ForeignInputRow::fx_city_temperature_tokyo();
        let r = Row::fx_city_temperature_tokyo();
        assert_eq!(fr.into_row::<TestDI>(stream).unwrap(), r);
    }
}
