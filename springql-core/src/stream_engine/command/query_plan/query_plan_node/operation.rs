use chrono::Duration;

use crate::pipeline::name::StreamName;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum QueryPlanOperation {
    Collect { stream: StreamName },
    TimeBasedSlidingWindow { lower_bound: Duration },
}
