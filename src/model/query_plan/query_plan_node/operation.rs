use chrono::Duration;

use crate::model::name::StreamName;

/// Leaf operations, which generates rows from a stream
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum LeafOperation {
    Collect { stream: StreamName },
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum SlidingWindowOperation {
    TimeBased { lower_bound: Duration },
}
