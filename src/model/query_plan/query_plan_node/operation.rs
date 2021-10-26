use chrono::Duration;

use crate::model::name::PumpName;

pub(crate) trait Operation {}

/// Leaf operations, which generates rows from a stream
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum LeafOperation {
    Collect { pump: PumpName },
}
impl Operation for LeafOperation {}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum SlidingWindowOperation {
    TimeBased { lower_bound: Duration },
}
impl Operation for SlidingWindowOperation {}
