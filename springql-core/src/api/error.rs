// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Error type.

pub mod foreign_info;

use thiserror::Error;

use crate::{api::error::foreign_info::ForeignInfo, time::TimeError};

/// Result type
pub type Result<T> = std::result::Result<T, SpringError>;

/// Error type
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum SpringError {
    #[error("I/O error related to foreign system: {foreign_info:?}")]
    ForeignIo {
        foreign_info: ForeignInfo,
        source: anyhow::Error,
    },

    #[error("Timeout when getting an input from foreign source: {foreign_info:?}")]
    ForeignSourceTimeout {
        foreign_info: ForeignInfo,
        source: anyhow::Error,
    },

    #[error("Timeout when getting an input from a stream ({task_name})")]
    InputTimeout {
        task_name: String,
        source: anyhow::Error,
    },

    #[error("I/O error inside SpringQL-core")]
    SpringQlCoreIo(anyhow::Error),

    #[error("another thread sharing the same resource got panic")]
    ThreadPoisoned(anyhow::Error),

    #[error("invalid config")]
    InvalidConfig { source: anyhow::Error },

    #[error("invalid option (key `{key:?}`, value `{value:?}`)")]
    InvalidOption {
        key: String,
        value: String,
        source: anyhow::Error,
    },

    #[error(r#"invalid format ("{s}")"#)]
    InvalidFormat { s: String, source: anyhow::Error },

    #[error("requested {resource} but its not available")]
    Unavailable {
        resource: String,
        source: anyhow::Error,
    },

    #[error("SQL error")]
    Sql(anyhow::Error),

    /// Occurs only when a value is fetched from a SpringSinkRow.
    #[error("unexpectedly got NULL")]
    Null {
        /// Column index
        i_col: usize,
    },

    #[error("Time conversion error {0}")]
    Time(TimeError),
}
