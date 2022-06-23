// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::{Result, SpringError},
    stream_engine::{Row, SpringValue, SqlValue},
};

/// Row object from an in memory sink queue.
#[derive(Debug)]
pub struct SpringSinkRow(Row);

impl SpringSinkRow {
    pub(crate) fn new(row: Row) -> Self {
        SpringSinkRow(row)
    }

    /// Get a i-th column value from the row.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::api::error::SpringError::Sql) when:
    ///   - Column index out of range
    /// - [SpringError::Null](crate::api::error::SpringError::Null) when:
    ///   - Column value is NULL
    pub fn get_not_null_by_index<T>(&self, i_col: usize) -> Result<T>
    where
        T: SpringValue,
    {
        let sql_value = self.0.get_by_index(i_col)?;

        match sql_value {
            SqlValue::Null => Err(SpringError::Null {
                stream_name: self.0.stream_model().name().clone(),
                i_col,
            }),
            SqlValue::NotNull(nn_sql_value) => nn_sql_value.unpack(),
        }
    }

    pub(crate) fn into_row(self) -> Row {
        self.0
    }
}
