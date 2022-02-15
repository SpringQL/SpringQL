// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Pipeline is the most important structural view of stream processing, similar to RDBMS's ER-diagram.
//!
//! Nodes are streams and edges are one of source reader, sink writer, or pump.
//! Some internal node may have multiple incoming edges. In this case, these edges share the same pump.
//!
//! A PipelineGraph has a "virtual root stream", who has outgoing edges to all source streams, to keep source readers.
//! It also has "virtual leaf streams", who has an incoming edge from each sink stream, to keep sink writers.

pub(crate) mod edge;
pub(crate) mod stream_node;

use std::{collections::HashMap, sync::Arc};

use petgraph::{
    graph::{DiGraph, EdgeReference, NodeIndex},
    visit::EdgeRef,
};

use self::{edge::Edge, stream_node::StreamNode};

use super::{
    name::StreamName, pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel, stream_model::StreamModel,
};
use crate::error::{Result, SpringError};
use anyhow::anyhow;

#[derive(Clone, Debug)]
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
    pub(super) fn add_stream(&mut self, stream: Arc<StreamModel>) -> Result<()> {
        let st_name = stream.name().clone();
        let st_node = self.graph.add_node(StreamNode::Stream(stream));
        let _ = self.stream_nodes.insert(st_name, st_node);
        Ok(())
    }

    pub(crate) fn get_stream(&self, name: &StreamName) -> Result<Arc<StreamModel>> {
        let node = self._find_stream(name)?;
        let stream_node = self.graph.node_weight(node).expect("index found");
        if let StreamNode::Stream(source_stream) = stream_node {
            Ok(source_stream.clone())
        } else {
            Err(SpringError::Sql(anyhow!(
                r#"stream "{}" is not a source stream"#,
                name
            )))
        }
    }

    /// Find all incoming edges of `edge_ref`'s upstream.
    pub(crate) fn upstream_edges(
        &self,
        edge_ref: &EdgeReference<Edge>,
    ) -> Vec<EdgeReference<Edge>> {
        let upstream_node = edge_ref.source();
        let upstream_edges = self
            .graph
            .edges_directed(upstream_node, petgraph::EdgeDirection::Incoming);
        upstream_edges.collect()
    }

    pub(super) fn all_sources(&self) -> Vec<&SourceReaderModel> {
        self.graph
            .edge_references()
            .filter_map(|edge| match edge.weight() {
                Edge::Pump { .. } | Edge::Sink(_) => None,
                Edge::Source(s) => Some(s),
            })
            .collect()
    }
    pub(super) fn all_sinks(&self) -> Vec<&SinkWriterModel> {
        self.graph
            .edge_references()
            .filter_map(|edge| match edge.weight() {
                Edge::Pump { .. } | Edge::Source(_) => None,
                Edge::Sink(s) => Some(s),
            })
            .collect()
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

            let _ = self.graph.add_edge(
                *upstream_node,
                *downstream_node,
                Edge::Pump {
                    pump_model: pump.clone(),
                    upstream: upstream_name.clone(),
                },
            );
        }

        Ok(())
    }

    fn _find_stream(&self, name: &StreamName) -> Result<NodeIndex> {
        Ok(*self.stream_nodes.get(name).ok_or_else(|| {
            SpringError::Sql(anyhow!(r#"stream "{}" does not exist in pipeline"#, name))
        })?)
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
            parent_sink_stream: from_stream.clone(),
        });
        let _ = self
            .graph
            .add_edge(*upstream_node, downstream_node, Edge::Sink(sink_writer));
        Ok(())
    }

    /// Just for `From<&PipelineGraph> for TaskGraph`
    pub(crate) fn as_petgraph(&self) -> &DiGraph<StreamNode, Edge> {
        &self.graph
    }
}
