// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::{Deserialize, Serialize};

use crate::pipeline::name::{ColumnName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct InsertPlan {
    stream: StreamName,
    column_order: Vec<ColumnName>,
}

impl InsertPlan {
    pub(crate) fn stream(&self) -> &StreamName {
        &self.stream
    }

    pub(crate) fn column_order(&self) -> &[ColumnName] {
        &self.column_order
    }
}
