pub(in crate::stream_engine) mod format;

use std::rc::Rc;

use self::format::json::JsonObject;

use crate::{
    dependency_injection::DependencyInjection, error::Result, model::stream_model::StreamModel,
};

use super::{column::stream_column::StreamColumns, row::Row};

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
        stream: Rc<StreamModel>,
    ) -> Result<Row> {
        // ForeignInputRow -> JsonObject -> HashMap<ColumnName, SqlValue> -> StreamColumns -> Row

        let column_values = self.0.into_column_values()?;
        let stream_columns = StreamColumns::new(stream, column_values)?;
        Ok(Row::new::<DI>(stream_columns))
    }
}

#[cfg(test)]
mod tests {
    use crate::{dependency_injection::test_di::TestDI, stream_engine::Timestamp};

    use super::*;

    #[test]
    fn test_json_into_row() {
        let stream = Rc::new(StreamModel::fx_city_temperature());

        let t = Timestamp::fx_ts1();
        let fr = ForeignInputRow::fx_tokyo(t);
        let r = Row::fx_tokyo(t);
        assert_eq!(fr.into_row::<TestDI>(stream).unwrap(), r);
    }
}
