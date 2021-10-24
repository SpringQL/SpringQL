use crate::{
    error::Result,
    model::{name::ColumnName, stream_model::StreamModel},
    stream_engine::executor::data::{timestamp::Timestamp, value::sql_value::SqlValue},
};
use std::rc::Rc;

/// Column values in a stream.
#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct StreamColumns {
    stream: Rc<StreamModel>,
}

impl StreamColumns {
    pub(in crate::stream_engine::executor) fn stream(&self) -> &StreamModel {
        self.stream.as_ref()
    }

    pub(in crate::stream_engine::executor) fn promoted_rowtime(&self) -> Option<Timestamp> {
        let rowtime_col = self.stream.rowtime()?;
        let rowtime_sql_value = self
            .get(rowtime_col)
            .expect("rowtime_col is set in stream definition, which must be validated");
        if let SqlValue::NotNull(v) = rowtime_sql_value {
            Some(v.unpack().expect("rowtime col must be TIMESTAMP type"))
        } else {
            panic!("rowtime_col must be NOT NULL")
        }
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
