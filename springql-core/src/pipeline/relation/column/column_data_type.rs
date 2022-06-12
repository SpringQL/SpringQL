// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{pipeline::name::ColumnName, pipeline::relation::sql_type::SqlType};

/// Column with data type.
#[derive(Clone, Eq, PartialEq, Hash, Debug, new)]
pub struct ColumnDataType {
    column: ColumnName,
    sql_type: SqlType,
    nullable: bool,
}

impl ColumnDataType {
    pub fn column_name(&self) -> &ColumnName {
        &self.column
    }

    pub fn sql_type(&self) -> &SqlType {
        &self.sql_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}
