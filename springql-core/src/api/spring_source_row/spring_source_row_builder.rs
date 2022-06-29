// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::{error::Result, SpringSourceRow},
    pipeline::ColumnName,
    stream_engine::{autonomous_executor::SchemalessRow, SpringValue},
};

/// Builder of `SpringSourceRow`.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct SpringSourceRowBuilder(SchemalessRow);

impl SpringSourceRowBuilder {
    /// Add a column to the source row.
    ///
    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `column_name` is already inserted.
    pub fn add_column<S, V>(mut self, column_name: S, value: V) -> Result<Self>
    where
        S: Into<String>,
        V: SpringValue,
    {
        let column_name = ColumnName::new(column_name.into());
        self.0.insert(column_name, value.into_sql_value())?;
        Ok(self)
    }

    /// Create a final source row.
    pub fn build(self) -> SpringSourceRow {
        SpringSourceRow::new(self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::SpringError;

    use super::*;

    #[test]
    fn test_add_column_error_sql() {
        let builder = SpringSourceRowBuilder::default();
        let builder = builder.add_column("a", 1).unwrap();
        assert!(matches!(
            builder.add_column("a", 1),
            Err(SpringError::Sql(_))
        ));
    }

    #[test]
    fn test_build() {
        let builder = SpringSourceRowBuilder::default();
        let _ = builder
            .add_column("a", 1i32)
            .unwrap()
            .add_column("b", "string".to_string())
            .unwrap()
            .build();
    }
}
