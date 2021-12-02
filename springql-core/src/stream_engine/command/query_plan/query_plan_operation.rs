// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::pipeline::name::{ColumnName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum QueryPlanOperation {
    Collect {
        stream: StreamName,
    },
    Projection {
        column_names: Vec<ColumnName>,
    },
    TimeBasedSlidingWindow {
        /// cannot use chrono::Duration here: <https://github.com/chronotope/chrono/issues/117>
        lower_bound: Duration,
    },
}
