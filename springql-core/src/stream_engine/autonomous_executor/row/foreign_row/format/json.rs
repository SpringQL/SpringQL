// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::Context;

use crate::{
    api::error::{Result, SpringError},
    pipeline::ColumnName,
    stream_engine::autonomous_executor::row::{column_values::ColumnValues, value::SqlValue},
};

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub struct JsonObject(serde_json::Value);

impl ToString for JsonObject {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl From<JsonObject> for serde_json::Value {
    fn from(j: JsonObject) -> Self {
        j.0
    }
}

impl JsonObject {
    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - `json_s` cannot be parsed as JSON
    pub(in crate::stream_engine::autonomous_executor) fn parse(json_s: &str) -> Result<Self> {
        let json_v = serde_json::from_str(json_s)
            .with_context(|| "failed to parse message from foreign stream as JSON")
            .map_err(|e| SpringError::InvalidFormat {
                s: json_s.to_string(),
                source: e,
            })?;

        Ok(Self::new(json_v))
    }

    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - Internal JSON cannot be mapped to SQL type (nested, for example).
    ///
    /// # TODO
    ///
    /// See stream.options to more intelligently parse JSON. <https://docs.sqlstream.com/sql-reference-guide/create-statements/createforeignstream/#parsing-json>
    pub fn into_column_values(self) -> Result<ColumnValues> {
        let json_object = self.0;

        let top_object = json_object
            .as_object()
            .context("top-level must be JSON object")
            .map_err(|e| SpringError::InvalidFormat {
                source: e,
                s: format!("{:?}", json_object),
            })?;

        top_object
            .into_iter()
            .fold(Ok(ColumnValues::default()), |acc, (k, v)| {
                let mut column_values = acc?;
                let (column_name, sql_value) = Self::to_column_value(k, v)?;
                column_values.insert(column_name, sql_value)?;
                Ok(column_values)
            })
    }

    fn to_column_value(
        json_key: &str,
        json_value: &serde_json::Value,
    ) -> Result<(ColumnName, SqlValue)> {
        let sql_value = SqlValue::try_from(json_value)?;
        let column_name = ColumnName::new(json_key.to_string());
        Ok((column_name, sql_value))
    }
}
