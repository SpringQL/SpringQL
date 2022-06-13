// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::cmp::max;

use crate::stream_engine::time::{
    SpringDuration, SpringEventDuration, SpringTimestamp, MIN_TIMESTAMP,
};

/// A watermark is held by each window.
///
/// ```text
/// watermark = max(ROWTIME) - allowed_delay
/// ```
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Watermark {
    max_rowtime: SpringTimestamp,
    allowed_delay: SpringEventDuration,
}

impl Watermark {
    pub fn new(allowed_delay: SpringEventDuration) -> Self {
        Self {
            max_rowtime: MIN_TIMESTAMP + allowed_delay.to_duration(), // to avoid overflow
            allowed_delay,
        }
    }

    pub fn as_timestamp(&self) -> SpringTimestamp {
        self.max_rowtime - self.allowed_delay.to_duration()
    }

    pub fn update(&mut self, rowtime: SpringTimestamp) {
        self.max_rowtime = max(rowtime, self.max_rowtime);
    }
}
