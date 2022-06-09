// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::time::duration::event_duration::SpringEventDuration;

/// Window parameters
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum WindowParameter {
    /// Time-based sliding window
    ///
    /// ```text
    /// length = 10sec, period = 5sec, allowed_delay = 0;
    ///
    /// pane1 |        |
    /// pane2      |         |
    /// pane3          |          |
    ///
    /// -----------------------------------> t
    ///      :00  :05  :10  :15  :20
    /// ```
    TimedSlidingWindow {
        length: SpringEventDuration,
        period: SpringEventDuration,
        allowed_delay: SpringEventDuration,
    },

    /// Time-based fixed window
    ///
    /// ```text
    /// length = 10sec, allowed_delay = 0;
    ///
    /// pane1 |         |
    /// pane2           |         |
    /// pane3                     |          |
    ///
    /// -----------------------------------> t
    ///      :00  :05  :10  :15  :20
    /// ```
    TimedFixedWindow {
        length: SpringEventDuration,
        allowed_delay: SpringEventDuration,
    },
}

impl WindowParameter {
    pub(crate) fn length(&self) -> SpringEventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { length, .. } => *length,
            WindowParameter::TimedFixedWindow { length, .. } => *length,
        }
    }

    pub(crate) fn period(&self) -> SpringEventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { period, .. } => *period,
            WindowParameter::TimedFixedWindow { length, .. } => *length,
        }
    }

    pub(crate) fn allowed_delay(&self) -> SpringEventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { allowed_delay, .. } => *allowed_delay,
            WindowParameter::TimedFixedWindow { allowed_delay, .. } => *allowed_delay,
        }
    }
}
