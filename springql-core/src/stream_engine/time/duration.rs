// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod event_duration;
mod wall_clock_duration;

pub use event_duration::SpringEventDuration;
pub use wall_clock_duration::{WallClockDuration, WallClockStopwatch};

use std::time::Duration as StdDuration;

use crate::time::Duration;

/// Duration based on event-time or process-time.
pub trait SpringDuration {
    fn as_std(&self) -> &StdDuration;
    fn from_std(duration: StdDuration) -> Self;

    fn from_secs(secs: u64) -> Self
    where
        Self: Sized,
    {
        let d = StdDuration::from_secs(secs);
        Self::from_std(d)
    }

    fn from_millis(millis: u64) -> Self
    where
        Self: Sized,
    {
        let d = StdDuration::from_millis(millis);
        Self::from_std(d)
    }

    fn from_micros(micros: u64) -> Self
    where
        Self: Sized,
    {
        let d = StdDuration::from_micros(micros);
        Self::from_std(d)
    }

    fn as_secs_f64(&self) -> f64 {
        self.as_std().as_secs_f64()
    }
    fn as_secs_f32(&self) -> f32 {
        self.as_std().as_secs_f32()
    }

    fn to_duration(&self) -> Duration {
        Duration::from_std(*self.as_std()).expect("too large duration for chrono")
    }
}
