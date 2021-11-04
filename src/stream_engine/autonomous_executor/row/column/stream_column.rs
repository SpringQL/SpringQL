use anyhow::{anyhow, Context};

use crate::{
    error::{Result, SpringError},
    pipeline::relation::column::column_definition::ColumnDefinition,
    pipeline::{name::ColumnName, stream_model::stream_shape::StreamShape},
    stream_engine::autonomous_executor::row::{
        column_values::ColumnValues, timestamp::Timestamp, value::sql_value::SqlValue,
    },
};
use std::{sync::Arc, vec};

/// Column values in a stream.
///
/// Should keep as small size as possible because all Row has this inside.
#[derive(Clone, PartialEq, Debug)]
pub(in crate::stream_engine::autonomous_executor) struct StreamColumns {
    stream_shape: Arc<StreamShape>,

    /// sorted to the same order as `stream_shape.columns()`.
    values: Vec<SqlValue>,
}

impl StreamColumns {
    /// Value may be type-casted to stream definition if possible.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `column_values` lacks any of `stream.columns()`.
    ///   - Type mismatch (and failed to convert type) with `stream_shape` and `column_values`.
    pub(in crate::stream_engine::autonomous_executor) fn new(
        stream_shape: Arc<StreamShape>,
        mut column_values: ColumnValues,
    ) -> Result<Self> {
        let values = stream_shape
            .columns()
            .iter()
            .map(|coldef| {
                let value = column_values.remove(coldef.column_data_type().column_name())?;
                Self::validate_or_try_convert_value_type(value, coldef)
            })
            .collect::<Result<Vec<SqlValue>>>()?;

        Ok(Self {
            stream_shape,
            values,
        })
    }

    pub(in crate::stream_engine::autonomous_executor) fn stream(&self) -> &StreamShape {
        self.stream_shape.as_ref()
    }

    pub(in crate::stream_engine::autonomous_executor) fn promoted_rowtime(
        &self,
    ) -> Option<Timestamp> {
        let rowtime_col = self.stream_shape.promoted_rowtime()?;
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
    pub(in crate::stream_engine::autonomous_executor) fn get(
        &self,
        column_name: &ColumnName,
    ) -> Result<&SqlValue> {
        let pos = self
            .stream_shape
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

    fn validate_or_try_convert_value_type(
        value: SqlValue,
        coldef: &ColumnDefinition,
    ) -> Result<SqlValue> {
        let cdt = coldef.column_data_type();

        match &value {
            SqlValue::NotNull(nn_value) => {
                if &nn_value.sql_type() == cdt.sql_type() {
                    Ok(value)
                } else {
                    let nn_value = nn_value
                    .try_convert(cdt.sql_type())
                    .with_context(|| format!(
                        r#"SQL type `{:?}` is expected for column "{}" from stream definition, while the value is {:?}"#,
                        cdt.sql_type(),
                        cdt.column_name(),
                        nn_value
                    ))
                    .map_err(SpringError::Sql)?;
                    Ok(SqlValue::NotNull(nn_value))
                }
            }
            SqlValue::Null => {
                if cdt.nullable() {
                    Ok(value)
                } else {
                    Err(SpringError::Sql(anyhow!(
                        r#"column "{}" cannot be NULL"#,
                        cdt.column_name(),
                    )))
                }
            }
        }
    }
}

impl IntoIterator for StreamColumns {
    type Item = (ColumnName, SqlValue);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.stream_shape
            .columns()
            .iter()
            .zip(self.values.into_iter())
            .map(|(coldef, sql_value)| (coldef.column_data_type().column_name().clone(), sql_value))
            .collect::<Vec<Self::Item>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_engine::autonomous_executor::row::value::sql_value::nn_sql_value::NnSqlValue;

    use super::*;

    #[test]
    fn test_new() {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Integer(21)),
            )
            .unwrap();

        let _ = StreamColumns::new(Arc::new(StreamShape::fx_city_temperature()), column_values)
            .unwrap();
    }

    #[test]
    fn test_column_lacks() {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
            )
            .unwrap();
        // lacks "temperature" column

        assert!(matches!(
            StreamColumns::new(Arc::new(StreamShape::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_type_mismatch() {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("21".to_string())), // not a INTEGER type
            )
            .unwrap();

        assert!(matches!(
            StreamColumns::new(Arc::new(StreamShape::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_not_null_mismatch() {
        let mut column_values = ColumnValues::default();
        column_values
            .insert(
                ColumnName::new("timestamp".to_string()),
                SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("city".to_string()),
                SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
            )
            .unwrap();
        column_values
            .insert(
                ColumnName::new("temperature".to_string()),
                SqlValue::Null, // NULL for NOT NULL column
            )
            .unwrap();

        assert!(matches!(
            StreamColumns::new(Arc::new(StreamShape::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }
}
