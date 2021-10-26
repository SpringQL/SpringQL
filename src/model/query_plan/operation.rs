use chrono::Duration;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum SlidingWindowOperation {
    TimeBased { lower_bound: Duration },
}
