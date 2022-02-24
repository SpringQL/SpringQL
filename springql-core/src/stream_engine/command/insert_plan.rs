// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

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
