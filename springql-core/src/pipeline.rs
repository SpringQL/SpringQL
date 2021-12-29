// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! - Pipeline
//!   - Stream
//!     - (native) Stream
//!     - Source/Sink Foreign Stream
//!   - Source Reader
//!   - Sink Writer
//!   - Pump

pub(crate) mod foreign_stream_model;
pub(crate) mod name;
pub(crate) mod option;
pub(crate) mod pipeline_graph;
pub(crate) mod pipeline_version;
pub(crate) mod pump_model;
pub(crate) mod relation;
pub(crate) mod sink_writer_model;
pub(crate) mod source_reader_model;
pub(crate) mod stream_model;

#[cfg(test)]
pub(crate) mod test_support;

use anyhow::anyhow;
use std::{collections::HashSet, sync::Arc};

use crate::{
    error::{Result, SpringError},
    pipeline::pipeline_graph::edge::Edge,
};
use serde::{Deserialize, Serialize};

use self::{
    foreign_stream_model::ForeignStreamModel, name::PumpName, pipeline_graph::PipelineGraph,
    pipeline_version::PipelineVersion, pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[cfg(test)] // TODO remove
use self::stream_model::StreamModel;

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
    ///   - Pump is not registered in pipeline
    pub(super) fn get_pump(&self, pump: &PumpName) -> Result<&PumpModel> {
        self.graph.get_pump(pump)
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
    ///   - Pump is not registered in pipeline
    pub(super) fn remove_pump(&mut self, name: &PumpName) -> Result<()> {
        self.update_version();
        self.deregister_name(name.as_ref())?;
        self.graph.remove_pump(name)
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
    #[cfg(test)] // TODO remove
    pub(super) fn add_stream(&mut self, stream: Arc<StreamModel>) -> Result<()> {
        self.update_version();
        self.register_name(stream.name().as_ref())?;
        self.graph.add_stream(stream)
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_source_reader(&mut self, source_reader: SourceReaderModel) -> Result<()> {
        self.update_version();
        self.graph.add_source_reader(source_reader)
    }
    /// # Failure
    ///
    /// TODO
    pub(super) fn add_sink_writer(&mut self, sink_writer: SinkWriterModel) -> Result<()> {
        self.update_version();
        self.graph.add_sink_writer(sink_writer)
    }

    pub(super) fn all_sources(&self) -> Vec<&SourceReaderModel> {
        self.graph
            .as_petgraph()
            .edge_references()
            .filter_map(|edge| match edge.weight() {
                Edge::Pump(_) | Edge::Sink(_) => None,
                Edge::Source(s) => Some(s),
            })
            .collect()
    }
    pub(super) fn all_sinks(&self) -> Vec<&SinkWriterModel> {
        self.graph
            .as_petgraph()
            .edge_references()
            .filter_map(|edge| match edge.weight() {
                Edge::Pump(_) | Edge::Source(_) => None,
                Edge::Sink(s) => Some(s),
            })
            .collect()
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

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name is not registered in pipeline
    fn deregister_name(&mut self, name: &str) -> Result<()> {
        if !self.object_names.remove(name) {
            Err(SpringError::Sql(anyhow!(
                r#"name "{}" is not registered in pipeline"#,
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
