// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::Result, pipeline::option::Options,
    stream_engine::autonomous_executor::row::foreign_row::foreign_source_row::ForeignSourceRow,
};
use std::fmt::Debug;

pub(in crate::stream_engine::autonomous_executor) mod net;
pub(in crate::stream_engine::autonomous_executor) mod source_reader_factory;
pub(in crate::stream_engine::autonomous_executor) mod source_reader_repository;

/// Instance of SourceReaderModel.
///
/// Since agents and servers may live as long as a program lives, source task cannot hold hold implementations of this trait.
pub(in crate::stream_engine::autonomous_executor) trait SourceReader:
    Debug + Sync + Send + 'static
{
    /// Blocks until the source subtask is ready to provide ForeignSourceRow.
    fn start(options: &Options) -> Result<Self>
    where
        Self: Sized;

    /// Returns currently available foreign row from foreign source.
    ///
    /// # Failure
    ///
    /// - [SpringError::ForeignSourceTimeout](crate::error::SpringError::ForeignSourceTimeout) when:
    ///   - Remote source does not provide row within timeout.
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - Failed to parse response from remote source.
    ///   - Unknown foreign error.
    fn next_row(&mut self) -> Result<ForeignSourceRow>;
}
