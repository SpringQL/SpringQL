// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine) mod wall_clock_stopwatch;

use std::{
    ops::{Add, Div, Mul, Sub},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::SpringDuration;

/// Real-time (wall-clock) duration.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct WallClockDuration(Duration);

impl SpringDuration for WallClockDuration {
    fn as_std(&self) -> &Duration {
        &self.0
    }

    fn from_std(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Add<WallClockDuration> for WallClockDuration {
    type Output = Self;

    fn add(self, rhs: WallClockDuration) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}
impl Sub<WallClockDuration> for WallClockDuration {
    type Output = Self;

    fn sub(self, rhs: WallClockDuration) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}
impl Mul<u32> for WallClockDuration {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}
impl Mul<f32> for WallClockDuration {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self::Output {
        Self(self.0.mul_f32(rhs))
    }
}
impl Div<u32> for WallClockDuration {
    type Output = Self;

    fn div(self, rhs: u32) -> Self::Output {
        Self(self.0 / rhs)
    }
}
