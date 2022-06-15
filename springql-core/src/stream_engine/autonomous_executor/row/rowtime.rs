// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{mem_size::MemSize, stream_engine::time::SpringTimestamp};

/// Either be an event-time or a process-time.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum RowTime {
    ProcessingTime(SpringTimestamp),
    EventTime(SpringTimestamp),
}

impl RowTime {
    pub fn as_timestamp(&self) -> SpringTimestamp {
        match self {
            Self::ProcessingTime(t) => *t,
            Self::EventTime(t) => *t,
        }
    }
}

impl MemSize for RowTime {
    fn mem_size(&self) -> usize {
        match self {
            Self::ProcessingTime(ts) => ts.mem_size(),
            Self::EventTime(ts) => ts.mem_size(),
        }
    }
}
