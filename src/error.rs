//! Error type.

use std::io;

use thiserror::Error;

/// Result type
pub type Result<T> = std::result::Result<T, SpringError>;

/// Error type
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum SpringError {
    #[error("I/O error")]
    Io(#[from] io::Error),

    #[error("invalid option (key `{key:?}`, value `{value:?}`)")]
    InvalidOption {
        key: String,
        value: String,
        source: anyhow::Error,
    },

    #[error(r#"invalid format ("{s}")"#)]
    InvalidFormat {
        s: String,
        source: anyhow::Error,
    },
}
