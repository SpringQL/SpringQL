use crate::{
    error::Result, model::option::Options,
    stream_engine::executor::data::foreign_row::ForeignInputRow,
};

pub(in crate::stream_engine::executor) mod net;

pub(in crate::stream_engine::executor) trait InputServerStandby<A: InputServerActive> {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized;

    /// Blocks until the server is ready to provide ForeignInputRow.
    fn start(self) -> Result<A>;
}

/// Active: ready to provide ForeignInputRow.
pub(in crate::stream_engine::executor) trait InputServerActive {
    /// Returns currently available foreign row.
    ///
    /// # Failure
    ///
    /// - [SpringError::ForeignInputTimeout](crate::error::SpringError::ForeignInputTimeout) when:
    ///   - Remote source does not provide row within timeout.
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - Failed to parse response from remote source.
    ///   - Unknown foreign error.
    fn next_row(&mut self) -> Result<ForeignInputRow>;
}
