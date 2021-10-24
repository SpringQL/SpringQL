use serde::{Deserialize, Serialize};

use crate::model::{name::ColumnName, sql_type::SqlType};

/// Column with data type.
#[derive(Eq, PartialEq, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnDataType {
    column: ColumnName,
    sql_type: SqlType,
    nullable: bool,
}
