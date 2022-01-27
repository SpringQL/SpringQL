// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Timestamp.

pub(crate) mod system_timestamp;

use anyhow::Context;
use chrono::{naive::MIN_DATETIME, Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Add, Sub},
    str::FromStr,
};

use crate::{
    error::SpringError,
    mem_size::{chrono_naive_date_time_overhead_size, MemSize},
};

/// The minimum possible `Timestamp`.
pub(crate) const MIN_TIMESTAMP: Timestamp = Timestamp(MIN_DATETIME);

const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

/// Timestamp in UTC. Serializable.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct Timestamp(#[serde(with = "datetime_format")] NaiveDateTime);

impl MemSize for Timestamp {
    fn mem_size(&self) -> usize {
        chrono_naive_date_time_overhead_size()
    }
}

impl Timestamp {
    /// Note: `2262-04-11T23:47:16.854775804` is the maximum possible timestamp because it uses nano-sec unixtime internally.
    pub(crate) fn floor(&self, resolution: Duration) -> Timestamp {
        let ts_nano = self.0.timestamp_nanos();
        let resolution_nano = resolution.num_nanoseconds().expect("no overflow");
        assert!(resolution_nano > 0);

        let floor_ts_nano = (ts_nano / resolution_nano) * resolution_nano;

        let floor_naive_date_time = {
            let floor_ts_secs = floor_ts_nano / 1_000_000_000;
            let floor_ts_nanos = floor_ts_nano % 1_000_000_000;
            NaiveDateTime::from_timestamp(floor_ts_secs, floor_ts_nanos as u32)
        };

        Timestamp(floor_naive_date_time)
    }

    /// Note: `2262-04-11T23:47:16.854775804` is the maximum possible timestamp because it uses nano-sec unixtime internally.
    pub(crate) fn ceil(&self, resolution: Duration) -> Timestamp {
        let floor = self.floor(resolution);
        if &floor == self {
            floor
        } else {
            floor + resolution
        }
    }
}

impl FromStr for Timestamp {
    type Err = SpringError;

    /// Parse as `"%Y-%m-%d %H:%M:%S%.9f"` format.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ndt = NaiveDateTime::parse_from_str(s, FORMAT)
            .with_context(|| format!(r#"failed to parse as {}"#, FORMAT))
            .map_err(|e| SpringError::InvalidFormat {
                s: s.to_string(),
                source: e,
            })?;
        Ok(Self(ndt))
    }
}

impl ToString for Timestamp {
    fn to_string(&self) -> String {
        self.0.format(FORMAT).to_string()
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}
impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        self.0 - rhs.0
    }
}

/// See: <https://serde.rs/custom-date-format.html>
mod datetime_format {
    use super::FORMAT;
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_floor() {
        fn t(ts: &str, resolution: Duration, expected: &str) {
            let ts = Timestamp::from_str(ts).unwrap();
            let expected = Timestamp::from_str(expected).unwrap();

            let actual = ts.floor(resolution);
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
            let ts = Timestamp::from_str(ts).unwrap();
            let expected = Timestamp::from_str(expected).unwrap();

            let actual = ts.ceil(resolution);
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
            let de: Timestamp = serde_json::from_str(&ser).unwrap();
            assert_eq!(de, t);
        }

        Ok(())
    }
}
