// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod blob;
mod boolean;
mod event_duration;
mod float;
mod int;
mod text;
mod timestamp;

use crate::{
    api::error::{Result, SpringError},
    stream_engine::time::{SpringEventDuration, SpringTimestamp},
};
use anyhow::anyhow;
use std::any::type_name;

use crate::stream_engine::autonomous_executor::row::value::sql_value::NnSqlValue;

/// Rust values can be unpacked from NnSqlValue back into them.
pub trait SpringValue: Sized {
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from i16
    fn try_from_i16(_: &i16) -> Result<Self> {
        Self::default_err("i16")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from i32
    fn try_from_i32(_: &i32) -> Result<Self> {
        Self::default_err("i32")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from i64
    fn try_from_i64(_: &i64) -> Result<Self> {
        Self::default_err("i64")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from u16
    fn try_from_u16(_: &u16) -> Result<Self> {
        Self::default_err("u16")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from u32
    fn try_from_u32(_: &u32) -> Result<Self> {
        Self::default_err("u32")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from u64
    fn try_from_u64(_: &u64) -> Result<Self> {
        Self::default_err("u64")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from f32
    fn try_from_f32(_: &f32) -> Result<Self> {
        Self::default_err("f32")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from String
    fn try_from_string(_: &str) -> Result<Self> {
        Self::default_err("String")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from Vec<u8>
    fn try_from_blob(_: &[u8]) -> Result<Self> {
        Self::default_err("Vec<u8>")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from bool
    fn try_from_bool(_: &bool) -> Result<Self> {
        Self::default_err("bool")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from Timestamp
    fn try_from_timestamp(_: &SpringTimestamp) -> Result<Self> {
        Self::default_err("Timestamp")
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - the type implementing SqlConvertible is not convertible from EventDuration
    fn try_from_duration(_: &SpringEventDuration) -> Result<Self> {
        Self::default_err("EventDuration")
    }

    #[doc(hidden)]
    fn default_err(from_type: &str) -> Result<Self> {
        Err(SpringError::Sql(anyhow!(
            "cannot convert {} -> {}",
            from_type,
            type_name::<Self>()
        )))
    }
}

/// Rust values which can be packed into NnSqlValue
pub trait ToNnSqlValue: Sized {
    /// Convert Rust type into strictly-matching SQL type.
    fn into_sql_value(self) -> NnSqlValue;
}
