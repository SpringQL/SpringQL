// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use super::Timestamp;

/// Wall-clock timestamp from system clock.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct SystemTimestamp;

impl SystemTimestamp {
    pub(crate) fn now() -> Timestamp {
        let t = chrono::offset::Utc::now().naive_utc();
        Timestamp::new(t)
    }
}
