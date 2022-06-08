// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::{anyhow, Context};

use crate::{
    api::error::{Result, SpringError},
    pipeline::relation::{column::column_definition::ColumnDefinition, sql_type::SqlType},
    pipeline::{name::ColumnName, relation::column::column_constraint::ColumnConstraint},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct StreamShape {
    cols: Vec<ColumnDefinition>,
    promoted_rowtime: Option<ColumnName>,
}

impl StreamShape {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - ROWTIME column in `cols` is not a `TIMESTAMP NOT NULL` type.
    ///   - 2 or more column have ROWTIME constraints
    pub(in crate) fn new(cols: Vec<ColumnDefinition>) -> Result<Self> {
        let promoted_rowtime = Self::extract_promoted_rowtime(&cols)?;

        let _ = if let Some(rowtime_col) = &promoted_rowtime {
            Self::validate_rowtime_column(rowtime_col, &cols)
        } else {
            Ok(())
        }?;

        Ok(Self {
            cols,
            promoted_rowtime,
        })
    }

    pub(crate) fn promoted_rowtime(&self) -> Option<&ColumnName> {
        self.promoted_rowtime.as_ref()
    }

    pub(crate) fn columns(&self) -> &[ColumnDefinition] {
        &self.cols
    }

    pub(crate) fn column_names(&self) -> Vec<ColumnName> {
        self.cols.iter().map(|c| c.column_name()).cloned().collect()
    }

    fn extract_promoted_rowtime(cols: &[ColumnDefinition]) -> Result<Option<ColumnName>> {
        let rowtime_cdts = cols
            .iter()
            .filter_map(|cd| {
                cd.column_constraints()
                    .iter()
                    .any(|cc| matches!(cc, ColumnConstraint::Rowtime))
                    .then(|| cd.column_data_type())
            })
            .collect::<Vec<_>>();

        if rowtime_cdts.is_empty() {
            Ok(None)
        } else if rowtime_cdts.len() == 1 {
            Ok(Some(rowtime_cdts[0].column_name().clone()))
        } else {
            Err(SpringError::Sql(anyhow!(
                "multiple columns ({}) have ROWTIME constraints",
                rowtime_cdts
                    .iter()
                    .map(|cdt| cdt.column_name().to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )))
        }
    }

    fn validate_rowtime_column(rowtime_col: &ColumnName, cols: &[ColumnDefinition]) -> Result<()> {
        let rowtime_coldef = cols
            .iter()
            .find(|coldef| coldef.column_data_type().column_name() == rowtime_col)
            .with_context(|| {
                format!(
                    r#"ROWTIME column "{}" is not in stream definition"#,
                    rowtime_col,
                )
            })
            .map_err(SpringError::Sql)?;

        if let SqlType::TimestampComparable = rowtime_coldef.column_data_type().sql_type() {
            Ok(())
        } else {
            Err(SpringError::Sql(anyhow!(
                r#"ROWTIME column "{}" is not TIMESTAMP type in stream definition"#,
                rowtime_col,
            )))
        }?;

        if rowtime_coldef.column_data_type().nullable() {
            Err(SpringError::Sql(anyhow!(
                r#"ROWTIME column "{}" must be NOT NULL in stream definition"#,
                rowtime_col,
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::relation::column::column_data_type::ColumnDataType;

    use super::*;

    #[test]
    fn test_rowtime() {
        let _ = StreamShape::new(vec![ColumnDefinition::fx_timestamp()]).expect("should succeed");
    }

    #[test]
    fn test_rowtime_not_timestamp_type() {
        assert!(matches!(
            StreamShape::new(vec![ColumnDefinition::new(
                ColumnDataType::new(
                    ColumnName::fx_timestamp(),
                    SqlType::integer(), // not a timestamp type
                    false
                ),
                vec![ColumnConstraint::Rowtime]
            )],)
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_rowtime_nullable_timestamp_type() {
        assert!(matches!(
            StreamShape::new(vec![ColumnDefinition::new(
                ColumnDataType::new(
                    ColumnName::fx_timestamp(),
                    SqlType::timestamp(),
                    true // nullable
                ),
                vec![ColumnConstraint::Rowtime]
            )],)
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }
}
