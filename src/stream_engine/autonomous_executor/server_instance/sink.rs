use crate::error::Result;
use crate::pipeline::option::Options;
use crate::stream_engine::autonomous_executor::row::foreign_row::foreign_sink_row::ForeignSinkRow;
use std::fmt::Debug;

pub(in crate::stream_engine) mod net;

pub(in crate::stream_engine) trait SinkServerInstance:
    Debug + Sync + Send + 'static
{
    /// Blocks until the server is ready to send ForeignSinkRow to foreign sink.
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
    fn send_row(&mut self, row: ForeignSinkRow) -> Result<()>;
}
