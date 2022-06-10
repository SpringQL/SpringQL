// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod in_memory_queue;
mod net;
mod sink_writer_factory;
mod sink_writer_repository;

pub use net::NetSinkWriter;
pub use sink_writer_repository::SinkWriterRepository;

use std::fmt::Debug;

use crate::{
    api::{error::Result, SpringSinkWriterConfig},
    pipeline::Options,
    stream_engine::Row,
};

/// Instance of SinkWriterModel.
///
/// Since agents and servers may live as long as a program lives, sink task cannot hold hold implementations of this trait.
pub trait SinkWriter: Debug + Sync + Send + 'static {
    /// Blocks until the sink subtask is ready to send SinkRow to foreign sink.
    fn start(options: &Options, config: &SpringSinkWriterConfig) -> Result<Self>
    where
        Self: Sized;

    /// # Failure
    ///
    /// - `SpringError::ForeignSourceTimeout` when:
    ///   - Remote sink does not accept row within timeout.
    /// - `SpringError::ForeignIo` when:
    ///   - Remote sink has failed to parse request.
    ///   - Unknown foreign error.
    fn send_row(&mut self, row: Row) -> Result<()>;
}
