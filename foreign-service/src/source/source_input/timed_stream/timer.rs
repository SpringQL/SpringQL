// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use chrono::{DateTime, Duration, Utc};

#[derive(Eq, PartialEq, Debug)]
pub struct Timer {
    real_initial_datetime: DateTime<Utc>,
    elapsed: Duration,

    virt_initial_datetime: DateTime<Utc>,
}

impl Timer {
    pub fn new(virt_initial_datetime: DateTime<Utc>) -> Self {
        let real_initial_datetime = Utc::now();
        let elapsed = Duration::seconds(0);
        Timer {
            real_initial_datetime,
            elapsed,
            virt_initial_datetime,
        }
    }

    pub fn virt_current_datetime(&mut self) -> DateTime<Utc> {
        self.update_clock();
        self.virt_initial_datetime + self.elapsed
    }

    fn update_clock(&mut self) {
        let now = Utc::now();
        self.elapsed = now - self.real_initial_datetime;
    }
}
