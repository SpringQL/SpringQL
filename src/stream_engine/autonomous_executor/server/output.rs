use crate::error::Result;
use crate::model::option::Options;
use crate::stream_engine::autonomous_executor::data::foreign_row::foreign_output_row::ForeignOutputRow;

pub(in crate::stream_engine::autonomous_executor) mod net;

pub(in crate::stream_engine::autonomous_executor) trait OutputServerStandby<A: OutputServerActive> {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized;

    /// Blocks until the server is ready to accept ForeignOutputRow.
    fn start(self) -> Result<A>;
}

/// Active: ready to accept ForeignOutputRow.
pub(in crate::stream_engine::autonomous_executor) trait OutputServerActive {
    /// # Failure
    ///
    /// - [SpringError::ForeignInputTimeout](crate::error::SpringError::ForeignInputTimeout) when:
    ///   - Remote sink does not accept row within timeout.
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - Remote sink has failed to parse request.
    ///   - Unknown foreign error.
    fn send_row(&mut self, row: ForeignOutputRow) -> Result<()>;
}
