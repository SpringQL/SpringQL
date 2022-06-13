#![doc = include_str!("lib.md")]
#![deny(missing_debug_implementations, missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub use springql_core::api::*;

/// error and result types for SpringQL
pub mod error {
    pub use springql_core::api::error::{Result, SpringError};
}

#[cfg(test)]
mod tests {}
