use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::pipeline::name::StreamName;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum QueryPlanOperation {
    Collect {
        stream: StreamName,
    },
    TimeBasedSlidingWindow {
        /// cannot use chrono::Duration here: <https://github.com/chronotope/chrono/issues/117>
        lower_bound: Duration,
    },
}
