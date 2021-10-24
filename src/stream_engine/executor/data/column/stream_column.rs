use crate::{
    error::Result, model::name::ColumnName,
    stream_engine::executor::data::value::sql_value::SqlValue, timestamp::Timestamp,
};

/// Column values in a stream.
#[derive(Eq, PartialEq, Debug, Default)]
pub(in crate::stream_engine::executor) struct StreamColumns;

impl StreamColumns {
    pub(in crate::stream_engine::executor) fn promoted_rowtime(&self) -> Option<&Timestamp> {
        todo!()
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - No column named `column_name` is found from this stream.
    pub(in crate::stream_engine::executor) fn get(
        &self,
        column_name: &ColumnName,
    ) -> Result<&SqlValue> {
        todo!()
    }
}
