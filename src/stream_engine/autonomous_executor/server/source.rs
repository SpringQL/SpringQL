use crate::{
    error::Result, model::option::Options,
    stream_engine::autonomous_executor::data::foreign_row::foreign_source_row::ForeignSourceRow,
};
use std::fmt::Debug;

pub(in crate::stream_engine::autonomous_executor) mod net;

pub(in crate::stream_engine::autonomous_executor) trait SourceServerStandby<A: SourceServerActive> {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized;

    /// Blocks until the server is ready to provide ForeignSourceRow.
    fn start(self) -> Result<A>;
}

/// Active: ready to provide ForeignSourceRow.
pub(in crate::stream_engine) trait SourceServerActive:
    Debug + Sync + Send
{
    /// Returns currently available foreign row.
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
