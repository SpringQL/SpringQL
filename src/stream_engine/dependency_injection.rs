#[cfg(test)]
pub(super) mod test_di;

use crate::stream_engine::{CurrentTimestamp, RowRepository, Scheduler};
use std::fmt::Debug;

/// Compile-time dependency injection.
///
/// FIXME remove dependent traits
pub(super) trait DependencyInjection: 'static {
    // Mainly for testable mock
    type CurrentTimestampType: CurrentTimestamp;

    // Autonomous executor
    type SchedulerType: Scheduler + Debug + Default + Sync + Send + 'static;
    type RowRepositoryType: RowRepository;
}
