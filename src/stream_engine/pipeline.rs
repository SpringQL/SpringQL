//! - Pipeline
//!   - Stream
//!     - (native) Stream
//!     - Source/Sink Foreign Stream
//!   - Source/Sink Server
//!   - Pump

pub(crate) mod foreign_stream_model;
pub(crate) mod pump_model;
pub(crate) mod server_model;
pub(crate) mod stream_model;

use crate::error::Result;
use serde::{Deserialize, Serialize};

use self::{
    foreign_stream_model::ForeignStreamModel, pump_model::PumpModel, server_model::ServerModel,
};

#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub(super) struct Pipeline;

impl Pipeline {
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of pump is already used in the same pipeline
    ///   - Name of upstream stream is not found in pipeline
    ///   - Name of downstream stream is not found in pipeline
    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        todo!()
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of foreign stream is already used in the same pipeline
    pub(super) fn add_foreign_stream(&mut self, foreign_stream: ForeignStreamModel) -> Result<()> {
        todo!()
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_source_server(&mut self, source_server: ServerModel) -> Result<()> {
        todo!()
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_sink_server(&mut self, sink_server: ServerModel) -> Result<()> {
        todo!()
    }
}
