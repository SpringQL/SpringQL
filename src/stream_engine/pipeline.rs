//! - Pipeline
//!   - Stream
//!     - (native) Stream
//!     - Source/Sink Foreign Stream
//!   - Source/Sink Server
//!   - Pump

pub(in crate::stream_engine) mod foreign_stream_model;
pub(in crate::stream_engine) mod pump_model;
pub(in crate::stream_engine) mod server_model;
pub(in crate::stream_engine) mod stream_model;

pub(in crate::stream_engine) mod pipeline_graph;
pub(in crate::stream_engine) mod pipeline_version;

use anyhow::anyhow;
use std::{collections::HashSet, sync::Arc};

use crate::error::{Result, SpringError};
use serde::{Deserialize, Serialize};

use self::{
    foreign_stream_model::ForeignStreamModel, pipeline_graph::PipelineGraph,
    pipeline_version::PipelineVersion, pump_model::PumpModel, server_model::ServerModel,
    stream_model::StreamModel,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Pipeline {
    version: PipelineVersion,
    object_names: HashSet<String>,
    graph: PipelineGraph,
}

impl Pipeline {
    pub(super) fn version(&self) -> PipelineVersion {
        self.version
    }

    pub(super) fn as_graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of pump is already used in the same pipeline
    ///   - Name of upstream stream is not found in pipeline
    ///   - Name of downstream stream is not found in pipeline
    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        self.update_version();
        self.register_name(pump.name().as_ref())?;
        self.graph.add_pump(pump)
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of foreign stream is already used in the same pipeline
    pub(super) fn add_foreign_stream(
        &mut self,
        foreign_stream: Arc<ForeignStreamModel>,
    ) -> Result<()> {
        self.update_version();
        self.register_name(foreign_stream.name().as_ref())?;
        self.graph.add_foreign_stream(foreign_stream)
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of stream is already used in the same pipeline
    pub(super) fn add_stream(&mut self, stream: Arc<StreamModel>) -> Result<()> {
        self.update_version();
        self.register_name(stream.name().as_ref())?;
        self.graph.add_stream(stream)
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_server(&mut self, server: ServerModel) -> Result<()> {
        self.update_version();
        self.graph.add_server(server)
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name is already used in the same pipeline
    fn register_name(&mut self, name: &str) -> Result<()> {
        if !self.object_names.insert(name.to_string()) {
            Err(SpringError::Sql(anyhow!(
                r#"name "{}" already exists in pipeline"#,
                name
            )))
        } else {
            Ok(())
        }
    }

    fn update_version(&mut self) {
        self.version.up();
    }
}
