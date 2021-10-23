//! Timestamp.

use anyhow::Context;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::SpringError;

const FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";

/// Timestamp in UTC. Serializable.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Timestamp(#[serde(with = "datetime_format")] NaiveDateTime);

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

mod datetime_format {
    use chrono::NaiveDateTime;
    use serde::{self, Deserialize, Deserializer, Serializer};

    use crate::timestamp::FORMAT;

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}
