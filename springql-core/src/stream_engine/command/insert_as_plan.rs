use serde::{Deserialize, Serialize};

use crate::pipeline::name::ColumnName;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct InsertAsPlan {
    insert_columns: Vec<ColumnName>,
}
