// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod prod_di;

#[cfg(test)]
pub(super) mod test_di;

use crate::stream_engine::{CurrentTimestamp, RowRepository, Scheduler};
use std::fmt::Debug;

/// Compile-time dependency injection.
///
/// FIXME remove dependent traits
pub(crate) trait DependencyInjection: Debug + 'static {
    // Mainly for testable mock
    type CurrentTimestampType: CurrentTimestamp;

    // Autonomous executor
    type SchedulerType: Scheduler;
    type RowRepositoryType: RowRepository;
}
