// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::stream_engine::time::duration::event_duration::EventDuration;

/// Window parameters
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum WindowParameter {
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
        length: EventDuration,
        period: EventDuration,
        allowed_delay: EventDuration,
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
        length: EventDuration,
        allowed_delay: EventDuration,
    },
}

impl WindowParameter {
    pub(crate) fn length(&self) -> EventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { length, .. } => *length,
            WindowParameter::TimedFixedWindow { length, .. } => *length,
        }
    }

    pub(crate) fn period(&self) -> EventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { period, .. } => *period,
            WindowParameter::TimedFixedWindow { length, .. } => *length,
        }
    }

    pub(crate) fn allowed_delay(&self) -> EventDuration {
        match self {
            WindowParameter::TimedSlidingWindow { allowed_delay, .. } => *allowed_delay,
            WindowParameter::TimedFixedWindow { allowed_delay, .. } => *allowed_delay,
        }
    }
}
