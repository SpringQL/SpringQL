mod wall_clock_stopwatch;

use std::{
    ops::{Add, Div, Mul, Sub},
    time::Duration,
};

use serde::{Deserialize, Serialize};

/// Real-time (wall-clock) duration.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct WallClockDuration(Duration);

impl WallClockDuration {
    pub(in crate::stream_engine) const fn from_millis(millis: u64) -> Self {
        let d = Duration::from_millis(millis);
        Self(d)
    }
    pub(in crate::stream_engine) const fn from_micros(micros: u64) -> Self {
        let d = Duration::from_micros(micros);
        Self(d)
    }

    pub(in crate::stream_engine) fn as_secs_f64(&self) -> f64 {
        self.0.as_secs_f64()
    }
    pub(in crate::stream_engine) fn as_secs_f32(&self) -> f32 {
        self.0.as_secs_f32()
    }
}

impl From<WallClockDuration> for Duration {
    fn from(w: WallClockDuration) -> Self {
        w.0
    }
}

impl From<Duration> for WallClockDuration {
    fn from(d: Duration) -> Self {
        Self(d)
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
