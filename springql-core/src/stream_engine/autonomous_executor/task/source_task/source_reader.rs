// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod net_client;
mod net_server;
mod source_reader_factory;
mod source_reader_repository;

pub use net_client::NetClientSourceReader;
pub use net_server::NetServerSourceReader;
pub use source_reader_repository::SourceReaderRepository;

use std::fmt::Debug;

use crate::{
    api::error::Result, api::SpringSourceReaderConfig, pipeline::Options,
    stream_engine::autonomous_executor::row::SourceRow,
};

/// Instance of SourceReaderModel.
///
/// Since agents and servers may live as long as a program lives, source task cannot hold hold implementations of this trait.
pub trait SourceReader: Debug + Sync + Send + 'static {
    /// Blocks until the source subtask is ready to provide SourceRow.
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self>
    where
        Self: Sized;

    /// Returns currently available row from foreign source.
    ///
    /// # Failure
    ///
    /// - `SpringError::ForeignSourceTimeout` when:
    ///   - Remote source does not provide row within timeout.
    /// - `SpringError::ForeignIo` when:
    ///   - Failed to parse response from remote source.
    ///   - Unknown foreign error.
    fn next_row(&mut self) -> Result<SourceRow>;
}
