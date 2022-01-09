use crate::stream_engine::time::timestamp::{system_timestamp::SystemTimestamp, Timestamp};

use super::WallClockDuration;

/// Real-time (wall-clock) stopwatch.
#[derive(Debug)]
pub(in crate::stream_engine) struct WallClockStopwatch {
    start_at: Timestamp,
}

impl WallClockStopwatch {
    pub(in crate::stream_engine) fn start() -> Self {
        let start_at = SystemTimestamp::now();
        Self { start_at }
    }

    /// # Panics
    ///
    /// If current system clock is smaller than start time.
    pub(in crate::stream_engine) fn stop(&self) -> WallClockDuration {
        let stop_at = SystemTimestamp::now();
        assert!(stop_at >= self.start_at);

        let duration = stop_at - self.start_at;
        WallClockDuration::from(duration.to_std().expect("chrono to_std"))
    }
}
