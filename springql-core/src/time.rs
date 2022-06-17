// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use time::{macros::format_description, UtcOffset};

#[derive(Debug, thiserror::Error)]
pub enum TimeError {
    #[error("Value out of range {0}")]
    OutOfRange(String),
    #[error("Parse Error {0}")]
    ParseError(#[from] time::error::Parse),
    #[error("Fomatting Error {0}")]
    FormatError(#[from] time::error::Format),
    #[error("Overflow {0}")]
    OverflowError(#[from] time::error::ConversionRange),
    #[error("Range {0}")]
    ComponentRange(#[from] time::error::ComponentRange),
}

#[derive(Debug, Copy, Clone)]
pub struct Duration(time::Duration);

impl Duration {
    pub fn seconds(seconds: i64) -> Self {
        Duration(time::Duration::seconds(seconds))
    }

    pub fn minutes(minutes: i64) -> Self {
        Duration(time::Duration::minutes(minutes))
    }

    pub fn hours(hours: i64) -> Self {
        Duration(time::Duration::hours(hours))
    }

    pub fn days(days: i64) -> Self {
        Duration(time::Duration::days(days))
    }

    pub fn milliseconds(milliseconds: i64) -> Self {
        Duration(time::Duration::milliseconds(milliseconds))
    }

    pub fn microseconds(microseconds: i64) -> Self {
        Duration(time::Duration::microseconds(microseconds))
    }

    pub fn nanoseconds(nanos: i64) -> Self {
        Duration(time::Duration::nanoseconds(nanos))
    }

    pub fn from_std(std_duration: std::time::Duration) -> Result<Self, TimeError> {
        Ok(Duration(time::Duration::try_from(std_duration)?))
    }

    pub fn to_std(self) -> Result<std::time::Duration, TimeError> {
        Ok(std::time::Duration::try_from(self.0)?)
    }

    pub fn num_nanoseconds(&self) -> i128 {
        self.0.whole_nanoseconds()
    }
}

impl Add for Duration {
    type Output = Duration;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

pub struct DateTime(time::OffsetDateTime);

impl DateTime {
    pub fn parse_from_rfc3339(s: &str) -> Result<Self, TimeError> {
        Ok(Self(time::OffsetDateTime::parse(
            s,
            &time::format_description::well_known::Rfc3339,
        )?))
    }

    pub fn naive_utc(&self) -> NaiveDateTime {
        NaiveDateTime(to_primitive(self.0))
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct NaiveDateTime(#[serde(with = "datetime_format")] time::PrimitiveDateTime);

pub const MIN_DATETIME: NaiveDateTime = NaiveDateTime(time::PrimitiveDateTime::MIN);

const FORMAT_DESCRIPTION: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]");

fn parse_to_primitive(s: &str) -> Result<time::PrimitiveDateTime, TimeError> {
    Ok(time::PrimitiveDateTime::parse(s, FORMAT_DESCRIPTION)?)
}

fn format_primitive(pri: &time::PrimitiveDateTime) -> Result<String, TimeError> {
    Ok(pri.format(FORMAT_DESCRIPTION)?)
}

fn to_primitive(odt: time::OffsetDateTime) -> time::PrimitiveDateTime {
    let udt = odt.to_offset(UtcOffset::UTC);
    time::PrimitiveDateTime::new(udt.date(), udt.time())
}

impl NaiveDateTime {
    pub fn utc_now() -> Self {
        let utc_now = time::OffsetDateTime::now_utc();
        Self(to_primitive(utc_now))
    }
    pub fn timestamp_nanos(&self) -> i128 {
        let ofs_time = self.0.assume_utc();
        ofs_time.unix_timestamp_nanos()
    }

    pub fn from_timestamp(secs: i64, nsecs: u32) -> Result<Self, TimeError> {
        let timestamp = ((secs as i128) * 1_000_000_000) + (nsecs as i128);
        let odt = time::OffsetDateTime::from_unix_timestamp_nanos(timestamp)?;
        Ok(Self(to_primitive(odt)))
    }

    pub fn parse_from_str(s: &str) -> Result<Self, TimeError> {
        Ok(Self(parse_to_primitive(s)?))
    }

    pub fn format(&self) -> String {
        format_primitive(&self.0).unwrap() // TODO: avoid panic
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
        Duration(self.0 - rhs.0)
    }
}

/// See: <https://serde.rs/custom-date-format.html>
mod datetime_format {
    use super::{format_primitive, parse_to_primitive};
    use time::PrimitiveDateTime;

    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &PrimitiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format_primitive(date).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PrimitiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_to_primitive(&s).map_err(serde::de::Error::custom)
    }
}
