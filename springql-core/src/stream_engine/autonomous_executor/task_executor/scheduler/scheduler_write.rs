// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::{Arc, RwLock, RwLockWriteGuard};

use crate::stream_engine::dependency_injection::DependencyInjection;

/// Writer of scheduler.
///
/// Writer: Main thread. Write on pipeline update.
#[derive(Clone, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct SchedulerWrite<DI>(
    Arc<RwLock<DI::SchedulerType>>,
)
where
    DI: DependencyInjection;

impl<DI> SchedulerWrite<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine::autonomous_executor) fn write_lock(
        &self,
    ) -> RwLockWriteGuard<'_, DI::SchedulerType> {
        self.0
            .write()
            .expect("another thread sharing the same scheduler must not panic")
    }
}
