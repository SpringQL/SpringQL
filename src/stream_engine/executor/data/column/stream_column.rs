use anyhow::{anyhow, Context};

use crate::{
    error::{Result, SpringError},
    model::{
        column::column_definition::ColumnDefinition, name::ColumnName, stream_model::StreamModel,
    },
    stream_engine::executor::data::{timestamp::Timestamp, value::sql_value::SqlValue},
};
use std::{collections::HashMap, rc::Rc};

/// Column values in a stream.
#[derive(PartialEq, Debug)]
pub(in crate::stream_engine::executor) struct StreamColumns {
    stream: Rc<StreamModel>,

    /// sorted to the same order as `stream.columns()`.
    values: Vec<SqlValue>,
}

impl StreamColumns {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `column_values` lacks any of `stream.columns()`.
    ///   - Type mismatch with `stream` and `column_values`.
    pub(in crate::stream_engine::executor) fn _new(
        stream: Rc<StreamModel>,
        mut column_values: HashMap<ColumnName, SqlValue>,
    ) -> Result<Self> {
        let values = stream
            .columns()
            .iter()
            .map(|coldef| {
                let value = column_values
                    .remove(coldef.column_data_type().column_name())
                    .with_context(|| {
                        format!(
                            r#"column "{}" not found from `column_values`"#,
                            coldef.column_data_type().column_name()
                        )
                    })
                    .map_err(SpringError::Sql)?;

                Self::validate_value_with_type(value, coldef)
            })
            .collect::<Result<Vec<SqlValue>>>()?;

        Ok(Self { stream, values })
    }

    pub(in crate::stream_engine::executor) fn _stream(&self) -> &StreamModel {
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

    fn validate_value_with_type(value: SqlValue, coldef: &ColumnDefinition) -> Result<SqlValue> {
        match &value {
            SqlValue::NotNull(nn_value) => {
                if &nn_value.sql_type() == coldef.column_data_type()._sql_type() {
                    Ok(value)
                } else {
                    Err(SpringError::Sql(anyhow!(
                        r#"SQL type `{:?}` is expected for column "{}" from stream definition, while the value is {}"#,
                        coldef.column_data_type()._sql_type(),
                        coldef.column_data_type().column_name(),
                        nn_value
                    )))
                }
            }
            SqlValue::Null => {
                if coldef.column_data_type()._nullable() {
                    Ok(value)
                } else {
                    Err(SpringError::Sql(anyhow!(
                        r#"column "{}" cannot be NULL"#,
                        coldef.column_data_type().column_name(),
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_engine::executor::data::value::sql_value::nn_sql_value::NnSqlValue;

    use super::*;

    #[test]
    fn test_column_lacks() {
        let mut column_values = HashMap::new();
        column_values.insert(
            ColumnName::new("timestamp".to_string()),
            SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
        );
        column_values.insert(
            ColumnName::new("city".to_string()),
            SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
        );
        // lacks "temperature" column

        assert!(matches!(
            StreamColumns::_new(Rc::new(StreamModel::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_type_mismatch() {
        let mut column_values = HashMap::new();
        column_values.insert(
            ColumnName::new("timestamp".to_string()),
            SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
        );
        column_values.insert(
            ColumnName::new("city".to_string()),
            SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
        );
        column_values.insert(
            ColumnName::new("temperature".to_string()),
            SqlValue::NotNull(NnSqlValue::Text("21".to_string())), // not a INTEGER type
        );

        assert!(matches!(
            StreamColumns::_new(Rc::new(StreamModel::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }

    #[test]
    fn test_not_null_mismatch() {
        let mut column_values = HashMap::new();
        column_values.insert(
            ColumnName::new("timestamp".to_string()),
            SqlValue::NotNull(NnSqlValue::Timestamp(Timestamp::fx_ts1())),
        );
        column_values.insert(
            ColumnName::new("city".to_string()),
            SqlValue::NotNull(NnSqlValue::Text("Tokyo".to_string())),
        );
        column_values.insert(
            ColumnName::new("temperature".to_string()),
            SqlValue::Null, // NULL for NOT NULL column
        );

        assert!(matches!(
            StreamColumns::_new(Rc::new(StreamModel::fx_city_temperature()), column_values)
                .unwrap_err(),
            SpringError::Sql(_)
        ));
    }
}
