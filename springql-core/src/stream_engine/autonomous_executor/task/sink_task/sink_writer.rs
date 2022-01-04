// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::option::Options;
use crate::stream_engine::autonomous_executor::row::foreign_row::sink_row::SinkRow;
use std::fmt::Debug;

pub(in crate::stream_engine::autonomous_executor) mod in_memory_queue;
pub(in crate::stream_engine::autonomous_executor) mod net;
pub(in crate::stream_engine::autonomous_executor) mod sink_writer_factory;
pub(in crate::stream_engine::autonomous_executor) mod sink_writer_repository;

/// Instance of SinkWriterModel.
///
/// Since agents and servers may live as long as a program lives, sink task cannot hold hold implementations of this trait.
pub(in crate::stream_engine) trait SinkWriter:
    Debug + Sync + Send + 'static
{
    /// Blocks until the sink subtask is ready to send ForeignSinkRow to foreign sink.
    fn start(options: &Options) -> Result<Self>
    where
        Self: Sized;

    /// # Failure
    ///
    /// - [SpringError::ForeignSourceTimeout](crate::error::SpringError::ForeignSourceTimeout) when:
    ///   - Remote sink does not accept row within timeout.
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - Remote sink has failed to parse request.
    ///   - Unknown foreign error.
    fn send_row(&mut self, row: SinkRow) -> Result<()>;
}
