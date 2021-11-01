use crate::error::Result;
use crate::model::option::Options;
use crate::stream_engine::autonomous_executor::data::foreign_row::foreign_sink_row::ForeignSinkRow;

pub(in crate::stream_engine::autonomous_executor) mod net;

pub(in crate::stream_engine::autonomous_executor) trait SinkServerStandby {
    type Act: SinkServerActive;

    fn new(options: &Options) -> Result<Self>
    where
        Self: Sized;

    /// Blocks until the server is ready to accept ForeignSinkRow.
    fn start(self) -> Result<Self::Act>;
}

/// Active: ready to accept ForeignSinkRow.
pub(in crate::stream_engine::autonomous_executor) trait SinkServerActive {
    /// # Failure
    ///
    /// - [SpringError::ForeignSourceTimeout](crate::error::SpringError::ForeignSourceTimeout) when:
    ///   - Remote sink does not accept row within timeout.
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - Remote sink has failed to parse request.
    ///   - Unknown foreign error.
    fn send_row(&mut self, row: ForeignSinkRow) -> Result<()>;
}
