//! Error type.

use std::io;

use thiserror::Error;

/// Error type
#[allow(missing_docs)]
#[derive(Debug, Error)]
enum SpringError {
    #[error("I/O error")]
    Io(#[from] io::Error),
}
