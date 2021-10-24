use serde::{Deserialize, Serialize};

use crate::model::{name::ColumnName, sql_type::SqlType};

/// Column with data type.
#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnDataType {
    column: ColumnName,
    sql_type: SqlType,
    nullable: bool,
}

impl ColumnDataType {
    pub(crate) fn column_name(&self) -> &ColumnName {
        &self.column
    }

    pub(crate) fn _sql_type(&self) -> &SqlType {
        &self.sql_type
    }

    pub(crate) fn _nullable(&self) -> bool {
        self.nullable
    }
}
