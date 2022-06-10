// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("pipeline.md")]

mod field;
mod name;
mod option;
mod pipeline_graph;
mod pipeline_version;
pub(crate) mod pump_model;
pub(crate) mod relation;
pub(crate) mod sink_writer_model;
pub(crate) mod source_reader_model;
pub(crate) mod stream_model;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use field::{ColumnReference, Field};
pub(crate) use name::{
    AggrAlias, ColumnName, CorrelationAlias, PumpName, QueueName, SinkWriterName, SourceReaderName,
    StreamName, ValueAlias,
};
pub(crate) use option::{
    InMemoryQueueOptions, NetClientOptions, NetProtocol, NetServerOptions, Options, OptionsBuilder,
};
pub(crate) use pipeline_graph::{Edge, PipelineGraph};
pub(crate) use pipeline_version::PipelineVersion;

use std::{collections::HashSet, sync::Arc};

use anyhow::anyhow;

use crate::{
    api::error::{Result, SpringError},
    pipeline::{
        pump_model::PumpModel, sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel, stream_model::StreamModel,
    },
};

#[derive(Clone, Debug)]
pub struct Pipeline {
    version: PipelineVersion,
    object_names: HashSet<String>,
    graph: PipelineGraph,
}

impl Pipeline {
    pub(super) fn new(version: PipelineVersion) -> Self {
        Self {
            version,
            object_names: HashSet::default(),
            graph: PipelineGraph::default(),
        }
    }

    pub(super) fn version(&self) -> PipelineVersion {
        self.version
    }

    pub(super) fn as_graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - Stream is not registered in pipeline
    pub(super) fn get_stream(&self, stream: &StreamName) -> Result<Arc<StreamModel>> {
        self.graph.get_stream(stream)
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
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
    /// - `SpringError::Sql` when:
    ///   - Name of stream is already used in the same pipeline
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
        self.graph.all_sources()
    }
    pub(super) fn all_sinks(&self) -> Vec<&SinkWriterModel> {
        self.graph.all_sinks()
    }

    /// # Failure
    ///
    /// - `SpringError::Sql` when:
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
