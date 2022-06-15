// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Timestamp.

mod system_timestamp;
pub use system_timestamp::SystemTimestamp;

use std::{
    ops::{Add, Sub},
    str::FromStr,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{
    api::error::{Result, SpringError},
    mem_size::{chrono_naive_date_time_overhead_size, MemSize},
    time::{DateTime, Duration, NaiveDateTime, MIN_DATETIME},
};

/// The minimum possible `Timestamp`.
pub const MIN_TIMESTAMP: SpringTimestamp = SpringTimestamp(MIN_DATETIME);

/// Timestamp in UTC. Serializable.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct SpringTimestamp(NaiveDateTime);

impl MemSize for SpringTimestamp {
    fn mem_size(&self) -> usize {
        chrono_naive_date_time_overhead_size()
    }
}

impl SpringTimestamp {
    /// Note: `2262-04-11T23:47:16.854775804` is the maximum possible timestamp because it uses nano-sec unixtime internally.
    pub fn floor(&self, resolution: Duration) -> Result<SpringTimestamp> {
        let ts_nano = self.0.timestamp_nanos();
        let resolution_nano = resolution.num_nanoseconds();
        assert!(resolution_nano > 0);

        let floor_ts_nano = (ts_nano / resolution_nano) * resolution_nano;

        let floor_naive_date_time = {
            let floor_ts_secs = floor_ts_nano / 1_000_000_000;
            let floor_ts_nanos = floor_ts_nano % 1_000_000_000;
            NaiveDateTime::from_timestamp(floor_ts_secs as i64, floor_ts_nanos as u32)
                .map_err(SpringError::Time)?
        };

        Ok(SpringTimestamp(floor_naive_date_time))
    }

    /// Note: `2262-04-11T23:47:16.854775804` is the maximum possible timestamp because it uses nano-sec unixtime internally.
    pub fn ceil(&self, resolution: Duration) -> Result<SpringTimestamp> {
        let floor = self.floor(resolution)?;
        if &floor == self {
            Ok(floor)
        } else {
            Ok(floor + resolution)
        }
    }

    fn try_parse_original(s: &str) -> Result<Self> {
        let ndt = NaiveDateTime::parse_from_str(s)
            .with_context(|| format!("failed to parse timestamp: {}", s))
            .map_err(|e| SpringError::InvalidFormat {
                s: s.to_string(),
                source: e,
            })?;
        Ok(SpringTimestamp(ndt))
    }
    fn try_parse_rfc3339(s: &str) -> Result<Self> {
        let dt = DateTime::parse_from_rfc3339(s)
            .with_context(|| format!("failed to parse timestamp: {}", s))
            .map_err(|e| SpringError::InvalidFormat {
                s: s.to_string(),
                source: e,
            })?;
        Ok(SpringTimestamp(dt.naive_utc()))
    }
}

impl FromStr for SpringTimestamp {
    type Err = SpringError;

    /// Parse as RFC-3339 or `"%Y-%m-%d %H:%M:%S%.9f"` format.
    fn from_str(s: &str) -> Result<Self> {
        Self::try_parse_rfc3339(s).or_else(|_| Self::try_parse_original(s))
    }
}

impl ToString for SpringTimestamp {
    fn to_string(&self) -> String {
        self.0.format()
    }
}

impl Add<Duration> for SpringTimestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}
impl Sub<Duration> for SpringTimestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl Sub<SpringTimestamp> for SpringTimestamp {
    type Output = Duration;

    fn sub(self, rhs: SpringTimestamp) -> Self::Output {
        self.0 - rhs.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::Result;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_floor() {
        fn t(ts: &str, resolution: Duration, expected: &str) {
            let ts = SpringTimestamp::from_str(ts).unwrap();
            let expected = SpringTimestamp::from_str(expected).unwrap();

            let actual = ts.floor(resolution).unwrap();
            assert_eq!(actual, expected);
        }

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::seconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::seconds(1),
            "2020-01-01 23:59:59.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::minutes(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::minutes(1),
            "2020-01-01 23:59:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::hours(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::hours(1),
            "2020-01-01 23:00:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::days(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::days(1),
            "2020-01-01 00:00:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::milliseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::milliseconds(1),
            "2020-01-01 23:59:59.999000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::microseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::microseconds(1),
            "2020-01-01 23:59:59.999999000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::nanoseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::nanoseconds(1),
            "2020-01-01 23:59:59.999999999",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::nanoseconds(10),
            "2020-01-01 23:59:59.999999990",
        );
    }

    #[test]
    fn test_ceil() {
        fn t(ts: &str, resolution: Duration, expected: &str) {
            let ts = SpringTimestamp::from_str(ts).unwrap();
            let expected = SpringTimestamp::from_str(expected).unwrap();

            let actual = ts.ceil(resolution).unwrap();
            assert_eq!(actual, expected);
        }

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::seconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::seconds(1),
            "2020-01-01 00:00:01.000000000",
        );
        t(
            "2020-01-01 23:59:59.999999999",
            Duration::seconds(1),
            "2020-01-02 00:00:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::minutes(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::minutes(1),
            "2020-01-01 00:01:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::hours(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::hours(1),
            "2020-01-01 01:00:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::days(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::days(1),
            "2020-01-02 00:00:00.000000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::milliseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::milliseconds(1),
            "2020-01-01 00:00:00.001000000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::microseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::microseconds(1),
            "2020-01-01 00:00:00.000001000",
        );

        t(
            "2020-01-01 00:00:00.000000000",
            Duration::nanoseconds(1),
            "2020-01-01 00:00:00.000000000",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::nanoseconds(1),
            "2020-01-01 00:00:00.000000001",
        );
        t(
            "2020-01-01 00:00:00.000000001",
            Duration::nanoseconds(10),
            "2020-01-01 00:00:00.000000010",
        );
    }

    #[test]
    fn test_timestamp_ser_de() -> Result<()> {
        let ts = vec![
            "2021-10-22 14:00:14.000000000",
            "2021-10-22 14:00:14.000000009",
        ]
        .into_iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<_>>>()?;

        for t in ts {
            let ser = serde_json::to_string(&t).unwrap();
            let de: SpringTimestamp = serde_json::from_str(&ser).unwrap();
            assert_eq!(de, t);
        }

        Ok(())
    }

    #[test]
    fn test_timestamp_parse_rfc3339() -> Result<()> {
        let ts_rfc3339: SpringTimestamp = "2020-01-01T09:12:34.56789+09:00".parse()?;
        let ts: SpringTimestamp = "2020-01-01 00:12:34.567890000".parse()?;
        assert_eq!(ts_rfc3339, ts);

        Ok(())
    }
}
