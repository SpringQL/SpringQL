use serde::{Deserialize, Serialize};

use crate::pipeline::name::{ColumnName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct InsertPlan {
    stream: StreamName,
    insert_columns: Vec<ColumnName>,
}

impl InsertPlan {
    pub(crate) fn stream(&self) -> &StreamName {
        &self.stream
    }
}
