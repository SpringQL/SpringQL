// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::time::{
    duration::{wall_clock_duration::WallClockDuration, SpringDuration},
    timestamp::{SpringTimestamp, SystemTimestamp},
};

/// Real-time (wall-clock) stopwatch.
#[derive(Debug)]
pub struct WallClockStopwatch {
    start_at: SpringTimestamp,
}

impl WallClockStopwatch {
    pub fn start() -> Self {
        let start_at = SystemTimestamp::now();
        Self { start_at }
    }

    /// # Panics
    ///
    /// If current system clock is smaller than start time.
    pub fn stop(&self) -> WallClockDuration {
        let stop_at = SystemTimestamp::now();
        assert!(stop_at >= self.start_at);

        let duration = stop_at - self.start_at;
        WallClockDuration::from_std(duration.to_std().expect("chrono to_std"))
    }
}
