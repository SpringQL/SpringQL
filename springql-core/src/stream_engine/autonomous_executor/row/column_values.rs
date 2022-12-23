// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::{Result, SpringError},
    pipeline::ColumnName,
};

use std::vec;

use anyhow::{anyhow, Context};

use crate::stream_engine::autonomous_executor::row::value::SqlValue;

#[derive(Clone, PartialEq, Debug, Default)]
pub struct ColumnValues(Vec<(ColumnName, SqlValue)>);

impl ColumnValues {
    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `k` is already inserted.
    pub fn insert(&mut self, k: ColumnName, v: SqlValue) -> Result<()> {
        if self.0.iter().any(|(col, _)| col == &k) {
            Err(SpringError::Sql(anyhow!(
                r#"column "{}" found twice in this ColumnValues"#,
                k
            )))
        } else {
            self.0.push((k, v));
            Ok(())
        }
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - Column index out of range
    pub fn get_by_index(&self, i_col: usize) -> Result<&SqlValue> {
        self.0.get(i_col).map(|(_, v)| v).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"column index {} out of range in this ColumnValues"#,
                i_col
            ))
        })
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `column_name` is not included.
    pub(crate) fn get_by_column_name(&self, column_name: &ColumnName) -> Result<&SqlValue> {
        let idx = self.find_idx(column_name)?;
        let (_, v) = self.0.get(idx).expect("valid index");
        Ok(v)
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `k` is not included.
    pub fn remove(&mut self, k: &ColumnName) -> Result<SqlValue> {
        let idx = self.find_idx(k)?;
        let (_, v) = self.0.swap_remove(idx);
        Ok(v)
    }

    fn find_idx(&self, column_name: &ColumnName) -> Result<usize> {
        self.0
            .iter()
            .enumerate()
            .find_map(|(i, (col, _))| (col == column_name).then_some(i))
            .with_context(|| {
                format!(
                    r#"column "{}" not found from this ColumnValues"#,
                    column_name
                )
            })
            .map_err(SpringError::Sql)
    }
}

impl IntoIterator for ColumnValues {
    type Item = (ColumnName, SqlValue);
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
