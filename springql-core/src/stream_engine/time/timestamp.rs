// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Timestamp.

pub(crate) mod system_timestamp;

use anyhow::Context;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Add, Sub},
    str::FromStr,
};

use crate::error::SpringError;

const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

/// Timestamp in UTC. Serializable.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct Timestamp(#[serde(with = "datetime_format")] NaiveDateTime);

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
