// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use chrono::{DateTime, Duration, Utc};

#[derive(Eq, PartialEq, Debug)]
pub(super) struct Timer {
    real_initial_datetime: DateTime<Utc>,
    elapsed: Duration,

    virt_initial_datetime: DateTime<Utc>,
}

impl Timer {
    pub(super) fn new(virt_initial_datetime: DateTime<Utc>) -> Self {
        let real_initial_datetime = Utc::now();
        let elapsed = Duration::seconds(0);
        Timer {
            real_initial_datetime,
            elapsed,
            virt_initial_datetime,
        }
    }

    pub(super) fn virt_current_datetime(&mut self) -> DateTime<Utc> {
        self.update_clock();
        self.virt_initial_datetime + self.elapsed
    }

    fn update_clock(&mut self) {
        let now = Utc::now();
        self.elapsed = now - self.real_initial_datetime;
    }
}
