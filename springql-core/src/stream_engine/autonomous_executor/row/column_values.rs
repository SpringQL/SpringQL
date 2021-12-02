// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::{Result, SpringError},
    pipeline::name::ColumnName,
};

use std::collections::HashMap;

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};

use super::value::sql_value::SqlValue;

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct ColumnValues(
    HashMap<ColumnName, SqlValue>,
);

impl ColumnValues {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `k` is already inserted.
    pub(in crate::stream_engine::autonomous_executor) fn insert(
        &mut self,
        k: ColumnName,
        v: SqlValue,
    ) -> Result<()> {
        if self.0.insert(k.clone(), v).is_some() {
            Err(SpringError::Sql(anyhow!(
                r#"column "{}" found twice in this ColumnValues"#,
                k
            )))
        } else {
            Ok(())
        }
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - `k` does not included.
    pub(in crate::stream_engine::autonomous_executor) fn remove(
        &mut self,
        k: &ColumnName,
    ) -> Result<SqlValue> {
        self.0
            .remove(k)
            .with_context(|| format!(r#"column "{}" not found from this ColumnValues"#, k))
            .map_err(SpringError::Sql)
    }
}
