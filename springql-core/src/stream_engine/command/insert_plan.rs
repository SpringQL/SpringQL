// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::pipeline::{ColumnName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub struct InsertPlan {
    stream: StreamName,
    column_order: Vec<ColumnName>,
}

impl InsertPlan {
    pub fn stream(&self) -> &StreamName {
        &self.stream
    }

    pub fn column_order(&self) -> &[ColumnName] {
        &self.column_order
    }
}
