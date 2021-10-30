#[cfg(test)]
pub(super) mod test_di;

use crate::stream_engine::{CurrentTimestamp, RowRepository, Scheduler};

/// Compile-time dependency injection.
pub(super) trait DependencyInjection {
    // Mainly for testable mock
    type CurrentTimestampType: CurrentTimestamp;

    // Autonomous executor
    type SchedulerType: Scheduler + Send + 'static;
    type RowRepositoryType: RowRepository;

    fn row_repository(&self) -> &Self::RowRepositoryType;
}
