// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::Timestamp;
use serde::{Deserialize, Serialize};

/// Wall-clock timestamp from system clock.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct SystemTimestamp;

impl SystemTimestamp {
    pub(crate) fn now() -> Timestamp {
        let t = chrono::offset::Utc::now().naive_utc();
        Timestamp::new(t)
    }
}
