// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{
    fmt::Display,
    mem::size_of,
    ops::{Add, Div, Mul, Sub},
    time::Duration,
};

use crate::mem_size::MemSize;

use super::SpringDuration;

/// Event-time duration.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SpringEventDuration(Duration);

impl MemSize for SpringEventDuration {
    fn mem_size(&self) -> usize {
        size_of::<u64>() + size_of::<u32>()
    }
}

impl SpringDuration for SpringEventDuration {
    fn as_std(&self) -> &Duration {
        &self.0
    }

    fn from_std(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Display for SpringEventDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} secs", self.0.as_secs())
    }
}

impl Add<SpringEventDuration> for SpringEventDuration {
    type Output = Self;

    fn add(self, rhs: SpringEventDuration) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}
impl Sub<SpringEventDuration> for SpringEventDuration {
    type Output = Self;

    fn sub(self, rhs: SpringEventDuration) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}
impl Mul<u32> for SpringEventDuration {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self::Output {
        Self(self.0 * rhs)
    }
}
impl Mul<f32> for SpringEventDuration {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self::Output {
        Self(self.0.mul_f32(rhs))
    }
}
impl Div<u32> for SpringEventDuration {
    type Output = Self;

    fn div(self, rhs: u32) -> Self::Output {
        Self(self.0 / rhs)
    }
}
