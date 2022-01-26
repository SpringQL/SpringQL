use std::{
    ops::{Add, Div, Mul, Sub},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::SpringDuration;

/// Event-time duration.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct EventDuration(Duration);

impl SpringDuration for EventDuration {
    fn as_std(&self) -> &Duration {
        &self.0
    }

    fn from_std(duration: Duration) -> Self {
        Self(duration)
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
