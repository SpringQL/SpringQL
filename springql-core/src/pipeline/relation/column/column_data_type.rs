// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::{Deserialize, Serialize};

use crate::{pipeline::name::ColumnName, pipeline::relation::sql_type::SqlType};

/// Column with data type.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnDataType {
    column: ColumnName,
    sql_type: SqlType,
    nullable: bool,
}

impl ColumnDataType {
    pub(crate) fn column_name(&self) -> &ColumnName {
        &self.column
    }

    pub(crate) fn sql_type(&self) -> &SqlType {
        &self.sql_type
    }

    pub(crate) fn nullable(&self) -> bool {
        self.nullable
    }
}
