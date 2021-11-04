use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use crate::{
    error::{Result, SpringError},
    model::sql_type::SqlType,
    pipeline::name::ColumnName,
};

use crate::model::column::column_definition::ColumnDefinition;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct StreamShape {
    cols: Vec<ColumnDefinition>,
    promoted_rowtime: Option<ColumnName>,
}

impl StreamShape {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `rowtime` column does not exist in `cols`.
    ///   - `rowtime` column in `cols` is not a `TIMESTAMP NOT NULL` type.
    pub(in crate) fn new(cols: Vec<ColumnDefinition>, rowtime: Option<ColumnName>) -> Result<Self> {
        let _ = if let Some(rowtime_col) = &rowtime {
            Self::validate_rowtime_column(rowtime_col, &cols)
        } else {
            Ok(())
        }?;

        Ok(Self {
            cols,
            promoted_rowtime: rowtime,
        })
    }

    pub(crate) fn promoted_rowtime(&self) -> Option<&ColumnName> {
        self.promoted_rowtime.as_ref()
    }

    pub(crate) fn columns(&self) -> &[ColumnDefinition] {
        &self.cols
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
    use crate::model::column::column_data_type::ColumnDataType;

    use super::*;

    #[test]
    fn test_rowtime() {
        let _ = StreamShape::new(
            vec![ColumnDefinition::fx_timestamp()],
            Some(ColumnName::new("timestamp".to_string())),
        )
        .expect("should succeed");
    }

    #[test]
    fn test_rowtime_not_found() {
        assert!(matches!(
            StreamShape::new(
                vec![ColumnDefinition::fx_timestamp(),],
                Some(ColumnName::new("invalid_ts_name".to_string())),
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_rowtime_not_timestamp_type() {
        assert!(matches!(
            StreamShape::new(
                vec![ColumnDefinition::new(ColumnDataType::new(
                    ColumnName::new("timestamp".to_string()),
                    SqlType::integer(), // not a timestamp type
                    false
                ))],
                Some(ColumnName::new("timestamp".to_string())),
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_rowtime_nullable_timestamp_type() {
        assert!(matches!(
            StreamShape::new(
                vec![ColumnDefinition::new(ColumnDataType::new(
                    ColumnName::new("timestamp".to_string()),
                    SqlType::timestamp(),
                    true // nullable
                ))],
                Some(ColumnName::new("timestamp".to_string())),
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }
}
