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
}
