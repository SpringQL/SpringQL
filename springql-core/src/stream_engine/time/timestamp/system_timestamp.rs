// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::time::timestamp::SpringTimestamp;

/// Wall-clock timestamp from system clock.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub struct SystemTimestamp;

impl SystemTimestamp {
    pub fn now() -> SpringTimestamp {
        let t = crate::time::NaiveDateTime::utc_now();
        SpringTimestamp::new(t)
    }
}
