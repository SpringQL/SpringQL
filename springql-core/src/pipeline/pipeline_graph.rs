// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! A PipelineGraph has a "virtual root stream", who has outgoing edges to all source foreign streams.
//! It also has "virtual leaf streams", who has an incoming edge from each sink foreign stream.

pub(crate) mod edge;
pub(crate) mod stream_node;

use std::{collections::HashMap, sync::Arc};

use petgraph::graph::{DiGraph, EdgeReference, NodeIndex};
use serde::{Deserialize, Serialize};

use self::{edge::Edge, stream_node::StreamNode};

use super::{
    name::{PumpName, StreamName},
    pump_model::PumpModel,
    sink_stream_model::SinkStreamModel,
    sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
    source_stream_model::SourceStreamModel,
};
use crate::error::{Result, SpringError};
use anyhow::anyhow;

#[cfg(test)] // TODO remove
use super::stream_model::StreamModel;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PipelineGraph {
    graph: DiGraph<StreamNode, Edge>,
    stream_nodes: HashMap<StreamName, NodeIndex>,
}

impl Default for PipelineGraph {
    fn default() -> Self {
        let mut graph = DiGraph::new();
        let virtual_root_node = graph.add_node(StreamNode::VirtualRoot);

        let mut stream_nodes = HashMap::new();
        stream_nodes.insert(StreamName::virtual_root(), virtual_root_node);

        Self {
            graph,
            stream_nodes,
        }
    }
}

impl PipelineGraph {
    #[cfg(test)] // TODO remove
    pub(super) fn add_stream(&mut self, stream: Arc<StreamModel>) -> Result<()> {
        let st_name = stream.name().clone();
        let st_node = self.graph.add_node(StreamNode::Native(stream));
        let _ = self.stream_nodes.insert(st_name, st_node);
        Ok(())
    }

    pub(super) fn add_source_stream(
        &mut self,
        source_stream: Arc<SourceStreamModel>,
    ) -> Result<()> {
        let stream_name = source_stream.name().clone();
        let node = self.graph.add_node(StreamNode::Source(source_stream));
        let _ = self.stream_nodes.insert(stream_name, node);
        Ok(())
    }

    pub(super) fn add_sink_stream(&mut self, sink_stream: Arc<SinkStreamModel>) -> Result<()> {
        let stream_name = sink_stream.name().clone();
        let node = self.graph.add_node(StreamNode::Sink(sink_stream));
        let _ = self.stream_nodes.insert(stream_name, node);
        Ok(())
    }

    pub(super) fn get_source_stream(&self, name: &StreamName) -> Result<Arc<SourceStreamModel>> {
        let node = self._find_stream(name)?;
        let stream_node = self.graph.node_weight(node).expect("index found");
        if let StreamNode::Source(source_stream) = stream_node {
            Ok(source_stream.clone())
        } else {
            Err(SpringError::Sql(anyhow!(
                r#"stream "{}" is not a source stream"#,
                name
            )))
        }
    }

    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        let pump = Arc::new(pump);

        let downstream_node = self.stream_nodes.get(pump.downstream()).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"downstream "{}" does not exist in pipeline"#,
                pump.downstream()
            ))
        })?;

        for upstream_name in pump.upstreams() {
            let upstream_node = self.stream_nodes.get(upstream_name).ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    r#"upstream "{}" does not exist in pipeline"#,
                    upstream_name
                ))
            })?;

            let _ = self
                .graph
                .add_edge(*upstream_node, *downstream_node, Edge::Pump(pump.clone()));
        }

        Ok(())
    }

    pub(crate) fn as_petgraph(&self) -> &DiGraph<StreamNode, Edge> {
        &self.graph
    }

    fn _find_stream(&self, name: &StreamName) -> Result<NodeIndex> {
        Ok(*self.stream_nodes.get(name).ok_or_else(|| {
            SpringError::Sql(anyhow!(r#"stream "{}" does not exist in pipeline"#, name))
        })?)
    }
    fn _find_pump(&self, name: &PumpName) -> Result<EdgeReference<Edge>> {
        self.graph
            .edge_references()
            .find_map(|edge| {
                if let Edge::Pump(pump) = edge.weight() {
                    (pump.name() == name).then(|| edge)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(r#"pump "{}" does not exist in pipeline"#, name))
            })
    }

    pub(super) fn add_source_reader(&mut self, source_reader: SourceReaderModel) -> Result<()> {
        let dest_stream = source_reader.dest_source_stream();

        let upstream_node = self
            .stream_nodes
            .get(&StreamName::virtual_root())
            .expect("virtual root always available");
        let downstream_node = self.stream_nodes.get(dest_stream).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"downstream "{}" does not exist in pipeline"#,
                dest_stream
            ))
        })?;
        let _ = self.graph.add_edge(
            *upstream_node,
            *downstream_node,
            Edge::Source(source_reader),
        );
        Ok(())
    }

    pub(super) fn add_sink_writer(&mut self, sink_writer: SinkWriterModel) -> Result<()> {
        let from_stream = sink_writer.from_sink_stream();

        let upstream_node = self.stream_nodes.get(from_stream).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"upstream "{}" does not exist in pipeline"#,
                from_stream
            ))
        })?;
        let downstream_node = self.graph.add_node(StreamNode::VirtualLeaf {
            parent_foreign_stream: from_stream.clone(),
        });
        let _ = self
            .graph
            .add_edge(*upstream_node, downstream_node, Edge::Sink(sink_writer));
        Ok(())
    }
}
