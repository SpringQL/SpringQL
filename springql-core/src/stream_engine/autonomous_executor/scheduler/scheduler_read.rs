// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::stream_engine::dependency_injection::DependencyInjection;

/// Reader of scheduler.
///
/// Reader: Worker threads. Read on task request.
#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct SchedulerRead<DI>(
    Arc<RwLock<DI::SchedulerType>>,
)
where
    DI: DependencyInjection;

impl<DI> SchedulerRead<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine::autonomous_executor) fn read_lock(
        &self,
    ) -> RwLockReadGuard<'_, DI::SchedulerType> {
        self.0
            .read()
            .expect("another thread sharing the same scheduler must not panic")
    }
}

impl<DI: DependencyInjection> Clone for SchedulerRead<DI> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
