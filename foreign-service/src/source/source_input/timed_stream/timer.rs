// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use time::{Duration, OffsetDateTime};

#[derive(Eq, PartialEq, Debug)]
pub struct Timer {
    real_initial_datetime: OffsetDateTime,
    elapsed: Duration,

    virt_initial_datetime: OffsetDateTime,
}

impl Timer {
    pub fn new(virt_initial_datetime: OffsetDateTime) -> Self {
        let real_initial_datetime = OffsetDateTime::now_utc();
        let elapsed = Duration::seconds(0);
        Timer {
            real_initial_datetime,
            elapsed,
            virt_initial_datetime,
        }
    }

    pub fn virt_current_datetime(&mut self) -> OffsetDateTime {
        self.update_clock();
        self.virt_initial_datetime + self.elapsed
    }

    fn update_clock(&mut self) {
        let now = OffsetDateTime::now_utc();
        self.elapsed = now - self.real_initial_datetime;
    }
}
