// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::{
    fmt::Display,
    mem::size_of,
    ops::{Add, Div, Mul, Sub},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::mem_size::MemSize;

use super::SpringDuration;

/// Event-time duration.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub(crate) struct EventDuration(Duration);

impl MemSize for EventDuration {
    fn mem_size(&self) -> usize {
        size_of::<u64>() + size_of::<u32>()
    }
}

impl SpringDuration for EventDuration {
    fn as_std(&self) -> &Duration {
        &self.0
    }

    fn from_std(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Display for EventDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} secs", self.0.as_secs())
    }
}

impl Add<EventDuration> for EventDuration {
    type Output = Self;

    fn add(self, rhs: EventDuration) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}
impl Sub<EventDuration> for EventDuration {
    type Output = Self;

    fn sub(self, rhs: EventDuration) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}
impl Mul<u32> for EventDuration {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}
impl Mul<f32> for EventDuration {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self::Output {
        Self(self.0.mul_f32(rhs))
    }
}
impl Div<u32> for EventDuration {
    type Output = Self;

    fn div(self, rhs: u32) -> Self::Output {
        Self(self.0 / rhs)
    }
}
