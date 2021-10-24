use anyhow::Context;

use crate::{
    error::{Result, SpringError},
    model::{name::ColumnName, stream_model::StreamModel},
    stream_engine::executor::data::{timestamp::Timestamp, value::sql_value::SqlValue},
};
use std::rc::Rc;

/// Column values in a stream.
#[derive(PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct StreamColumns {
    stream: Rc<StreamModel>,

    /// sorted to the same order as `stream.columns()`.
    values: Vec<SqlValue>,
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
        let pos = self
            .stream
            .columns()
            .iter()
            .position(|coldef| coldef.column_data_type().column_name() == column_name)
            .with_context(|| format!(r#"column "{}" not found"#, column_name))
            .map_err(SpringError::Sql)?;

        Ok(self
            .values
            .get(pos)
            .expect("self.values must be sorted to the same as self.stream.columns()"))
    }
}
