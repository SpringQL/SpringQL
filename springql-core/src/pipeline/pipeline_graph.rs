// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! A PipelineGraph has a "virtual root stream", who has outgoing edges to all source foreign streams.
//! It also has "virtual leaf streams", who has an incoming edge from each sink foreign stream.

pub(crate) mod edge;
pub(crate) mod stream_node;

use std::{collections::HashMap, sync::Arc};

use petgraph::{
    graph::{DiGraph, EdgeReference, NodeIndex},
    visit::EdgeRef,
};
use serde::{Deserialize, Serialize};

use self::{edge::Edge, stream_node::StreamNode};

use super::{
    foreign_stream_model::ForeignStreamModel,
    name::{PumpName, StreamName},
    pump_model::PumpModel,
    sink_writer::SinkWriter,
    source_reader::{source_reader_state::SourceReaderState, SourceReader},
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

    pub(super) fn add_foreign_stream(
        &mut self,
        foreign_stream: Arc<ForeignStreamModel>,
    ) -> Result<()> {
        let fst_name = foreign_stream.name().clone();
        let fst_node = self.graph.add_node(StreamNode::Foreign(foreign_stream));
        let _ = self.stream_nodes.insert(fst_name, fst_node);
        Ok(())
    }

    pub(super) fn get_pump(&self, name: &PumpName) -> Result<&PumpModel> {
        let edge = self._find_pump(name)?;
        if let Edge::Pump(pump) = edge.weight() {
            Ok(pump)
        } else {
            unreachable!()
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

    pub(super) fn remove_pump(&mut self, name: &PumpName) -> Result<()> {
        let edge_idx = {
            let edge = self._find_pump(name)?;
            edge.id()
        };
        self.graph.remove_edge(edge_idx);
        Ok(())
    }

    pub(crate) fn source_reader_state(
        &self,
        serving_foreign_stream: &StreamName,
    ) -> SourceReaderState {
        let fst_node = self
            .graph
            .node_indices()
            .find(|n| &self.graph[*n].name() == serving_foreign_stream)
            .unwrap();

        if self._at_least_one_started_path_to_sink(fst_node) {
            SourceReaderState::Started
        } else {
            SourceReaderState::Stopped
        }
    }

    pub(crate) fn as_petgraph(&self) -> &DiGraph<StreamNode, Edge> {
        &self.graph
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

    fn _at_least_one_started_path_to_sink(&self, node: NodeIndex) -> bool {
        let mut outgoing_edges = self
            .graph
            .edges_directed(node, petgraph::Direction::Outgoing);

        if outgoing_edges
            .clone()
            .any(|edge| matches!(edge.weight(), Edge::Sink(_)))
        {
            true
        } else {
            outgoing_edges.any(|started_edge| {
                let next_node = started_edge.target();
                self._at_least_one_started_path_to_sink(next_node)
            })
        }
    }

    pub(super) fn add_source_reader(&mut self, source_reader: SourceReader) -> Result<()> {
        let serving_to = source_reader.dest_foreign_stream();

        let upstream_node = self
            .stream_nodes
            .get(&StreamName::virtual_root())
            .expect("virtual root always available");
        let downstream_node = self.stream_nodes.get(serving_to.name()).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"downstream "{}" does not exist in pipeline"#,
                serving_to.name()
            ))
        })?;
        let _ = self.graph.add_edge(
            *upstream_node,
            *downstream_node,
            Edge::Source(source_reader),
        );
        Ok(())
    }

    pub(super) fn add_sink_writer(&mut self, sink_writer: SinkWriter) -> Result<()> {
        let serving_to = sink_writer.from_foreign_stream();

        let upstream_node = self.stream_nodes.get(serving_to.name()).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"upstream "{}" does not exist in pipeline"#,
                serving_to.name()
            ))
        })?;
        let downstream_node = self.graph.add_node(StreamNode::VirtualLeaf {
            parent_foreign_stream: serving_to.name().clone(),
        });
        let _ = self
            .graph
            .add_edge(*upstream_node, downstream_node, Edge::Sink(sink_writer));
        Ok(())
    }
}
