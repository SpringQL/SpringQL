// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::cmp::max;

use crate::stream_engine::time::{
    duration::{event_duration::SpringEventDuration, SpringDuration},
    timestamp::{SpringTimestamp, MIN_TIMESTAMP},
};

/// A watermark is held by each window.
///
/// ```text
/// watermark = max(ROWTIME) - allowed_delay
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub(in crate::stream_engine::autonomous_executor) struct Watermark {
    max_rowtime: SpringTimestamp,
    allowed_delay: SpringEventDuration,
}

impl Watermark {
    pub(in crate::stream_engine::autonomous_executor) fn new(allowed_delay: SpringEventDuration) -> Self {
        Self {
            max_rowtime: MIN_TIMESTAMP + allowed_delay.to_chrono(), // to avoid overflow
            allowed_delay,
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn as_timestamp(&self) -> SpringTimestamp {
        self.max_rowtime - self.allowed_delay.to_chrono()
    }

    pub(in crate::stream_engine::autonomous_executor) fn update(&mut self, rowtime: SpringTimestamp) {
        self.max_rowtime = max(rowtime, self.max_rowtime);
    }
}
