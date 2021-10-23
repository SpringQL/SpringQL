//! Error type.

pub mod foreign_info;
pub mod responsibility;

use thiserror::Error;

use self::{foreign_info::ForeignInfo, responsibility::SpringErrorResponsibility};

/// Result type
pub type Result<T> = std::result::Result<T, SpringError>;

/// Error type
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum SpringError {
    #[error("I/O error related to foreign system")]
    ForeignIo {
        foreign_info: ForeignInfo,
        source: anyhow::Error,
    },
    #[error("I/O error inside SpringQL-core")]
    SpringQlCoreIo(anyhow::Error),

    #[error("I/O error in foreign system")]
    ForeignInputTimeout {
        foreign_info: ForeignInfo,
        source: anyhow::Error,
    },

    #[error("invalid option (key `{key:?}`, value `{value:?}`)")]
    InvalidOption {
        key: String,
        value: String,
        source: anyhow::Error,
    },

    #[error(r#"invalid format ("{s}")"#)]
    InvalidFormat { s: String, source: anyhow::Error },
}

impl SpringError {
    /// Get who is responsible for the error.
    /// Used for error handling and bug reports.
    pub fn responsibility(&self) -> SpringErrorResponsibility {
        match self {
            SpringError::ForeignIo { .. } => SpringErrorResponsibility::Foreign,
            SpringError::SpringQlCoreIo(_) => SpringErrorResponsibility::SpringQlCore,
            SpringError::ForeignInputTimeout { .. } => SpringErrorResponsibility::SpringQlCore,
            SpringError::InvalidOption { .. } => SpringErrorResponsibility::Client,
            SpringError::InvalidFormat { .. } => SpringErrorResponsibility::Client,
        }
    }
}
