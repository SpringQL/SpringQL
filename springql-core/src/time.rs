use std::ops::{Add, Sub};

use chrono::{FixedOffset, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum TimeError {
    #[error("Value out of range {0}")]
    OutOfRange(String),
    #[error("Parse Error {0}")]
    ParseError(#[from] chrono::ParseError),
}

#[derive(Debug, Copy, Clone)]
pub struct Duration(chrono::Duration);

impl Duration {
    pub fn seconds(seconds: i64) -> Self {
        Duration(chrono::Duration::seconds(seconds))
    }

    pub fn minutes(minutes: i64) -> Self {
        Duration(chrono::Duration::minutes(minutes))
    }

    pub fn hours(hours: i64) -> Self {
        Duration(chrono::Duration::hours(hours))
    }

    pub fn days(days: i64) -> Self {
        Duration(chrono::Duration::days(days))
    }

    pub fn milliseconds(milliseconds: i64) -> Self {
        Duration(chrono::Duration::milliseconds(milliseconds))
    }

    pub fn microseconds(microseconds: i64) -> Self {
        Duration(chrono::Duration::microseconds(microseconds))
    }

    pub fn nanoseconds(nanos: i64) -> Self {
        Duration(chrono::Duration::nanoseconds(nanos))
    }

    pub fn from_std(std_duration: std::time::Duration) -> Result<Self, TimeError> {
        Ok(Duration(
            chrono::Duration::from_std(std_duration)
                .map_err(|e| TimeError::OutOfRange(e.to_string()))?,
        ))
    }

    pub fn to_std(&self) -> Result<std::time::Duration, TimeError> {
        Ok(self
            .0
            .to_std()
            .map_err(|e| TimeError::OutOfRange(e.to_string()))?)
    }

    pub fn num_nanoseconds(&self) -> Option<i64> {
        self.0.num_nanoseconds()
    }
}

impl Add for Duration {
    type Output = Duration;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

pub struct DateTime(chrono::DateTime<FixedOffset>);

impl DateTime {
    pub fn parse_from_rfc3339(s: &str) -> Result<Self, TimeError> {
        Ok(Self(chrono::DateTime::parse_from_rfc3339(s)?))
    }

    pub fn naive_utc(&self) -> NaiveDateTime {
        NaiveDateTime(self.0.naive_utc())
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct NaiveDateTime(#[serde(with = "datetime_format")] chrono::NaiveDateTime);

pub const MIN_DATETIME: NaiveDateTime = NaiveDateTime(chrono::naive::MIN_DATETIME);
const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

impl NaiveDateTime {
    pub fn utc_now() -> Self {
        Self(Utc::now().naive_utc())
    }
    pub fn timestamp_nanos(&self) -> i64 {
        self.0.timestamp_nanos()
    }

    pub fn from_timestamp(secs: i64, nsecs: u32) -> Self {
        Self(chrono::NaiveDateTime::from_timestamp(secs, nsecs))
    }

    pub fn parse_from_str(s: &str) -> Result<Self, TimeError> {
        Ok(Self(chrono::NaiveDateTime::parse_from_str(s, FORMAT)?))
    }

    pub fn format(&self) -> String {
        self.0.format(FORMAT).to_string()
    }
}

impl Add<Duration> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn add(self, rhs: Duration) -> NaiveDateTime {
        Self(self.0 + rhs.0)
    }
}

impl Sub<Duration> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Sub<NaiveDateTime> for NaiveDateTime {
    type Output = Duration;

    fn sub(self, rhs: NaiveDateTime) -> Duration {
        Duration(self.0.signed_duration_since(rhs.0))
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
