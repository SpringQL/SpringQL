use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use crate::{
    error::{Result, SpringError},
    model::sql_type::SqlType,
};

use super::{
    column::column_definition::ColumnDefinition,
    name::{ColumnName, StreamName},
};

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct StreamModel {
    name: StreamName,
    cols: Vec<ColumnDefinition>,
    rowtime: Option<ColumnName>,
}

impl StreamModel {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `rowtime` column does not exist in `cols`.
    ///   - `rowtime` column in `cols` is not a `TIMESTAMP NOT NULL` type.
    pub(in crate) fn new(
        name: StreamName,
        cols: Vec<ColumnDefinition>,
        rowtime: Option<ColumnName>,
    ) -> Result<Self> {
        let _ = if let Some(rowtime_col) = &rowtime {
            let rowtime_coldef = cols
                .iter()
                .find(|coldef| coldef.column_data_type().column_name() == rowtime_col)
                .with_context(|| {
                    format!(
                        r#"ROWTIME column "{}" is not in stream ("{}") definition"#,
                        rowtime_col, name,
                    )
                })
                .map_err(SpringError::Sql)?;

            if let SqlType::TimestampComparable = rowtime_coldef.column_data_type().sql_type() {
                Ok(())
            } else {
                Err(SpringError::Sql(anyhow!(
                    r#"ROWTIME column "{}" is not in stream ("{}") definition"#,
                    rowtime_col,
                    name,
                )))
            }?;

            if rowtime_coldef.column_data_type().nullable() {
                Err(SpringError::Sql(anyhow!(
                    r#"ROWTIME column "{}" is not in stream ("{}") definition"#,
                    rowtime_col,
                    name,
                )))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }?;

        Ok(Self {
            name,
            cols,
            rowtime,
        })
    }

    pub(crate) fn rowtime(&self) -> Option<&ColumnName> {
        self.rowtime.as_ref()
    }

    pub(crate) fn columns(&self) -> &[ColumnDefinition] {
        &self.cols
    }
}

#[cfg(test)]
mod tests {
    use crate::model::column::column_data_type::ColumnDataType;

    use super::*;

    #[test]
    fn test_rowtime_not_found() {
        assert!(matches!(
            StreamModel::new(
                StreamName::new("s".to_string()),
                vec![ColumnDefinition::fx_timestamp(),],
                Some(ColumnName::new("invalid_ts_name".to_string()))
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_rowtime_not_timestamp_type() {
        assert!(matches!(
            StreamModel::new(
                StreamName::new("s".to_string()),
                vec![ColumnDefinition::new(ColumnDataType::new(
                    ColumnName::new("timestamp".to_string()),
                    SqlType::integer(), // not a timestamp type
                    false
                ))],
                Some(ColumnName::new("timestamp".to_string()))
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_rowtime_nullable_timestamp_type() {
        assert!(matches!(
            StreamModel::new(
                StreamName::new("s".to_string()),
                vec![ColumnDefinition::new(ColumnDataType::new(
                    ColumnName::new("timestamp".to_string()),
                    SqlType::timestamp(),
                    false // nullable
                ))],
                Some(ColumnName::new("timestamp".to_string()))
            )
            .unwrap_err(),
            SpringError::Sql(_)
        ));
    }
}
