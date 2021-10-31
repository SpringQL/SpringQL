//! A PipelineGraph has a "virtual root stream", who has outgoing edges to all source foreign streams.
//! It also has "virtual leaf streams", who has an incoming edge from each sink foreign stream.

use std::collections::HashMap;

use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};

use super::{
    foreign_stream_model::{self, ForeignStreamModel},
    pump_model::PumpModel,
    server_model::ServerModel,
    stream_model::StreamModel,
};
use crate::{
    error::{Result, SpringError},
    model::name::StreamName,
    stream_engine::pipeline::server_model::server_type::ServerType,
};
use anyhow::anyhow;

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(super) enum StreamNode {
    Native(StreamModel),
    Foreign(ForeignStreamModel),
    VirtualRoot,
}

#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(super) enum PumpEdge {
    Pump(PumpModel),
    Source(ServerModel),
    Sink(ServerModel),
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PipelineGraph {
    graph: DiGraph<StreamNode, PumpEdge>,
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
    pub(super) fn add_foreign_stream(&mut self, foreign_stream: ForeignStreamModel) -> Result<()> {
        let fst_name = foreign_stream.name().clone();
        let fst_node = self.graph.add_node(StreamNode::Foreign(foreign_stream));
        let _ = self.stream_nodes.insert(fst_name, fst_node);
        Ok(())
    }

    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        let upstream_node = self.stream_nodes.get(pump.upstream()).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"upstream "{}" does not exist in pipeline"#,
                pump.upstream()
            ))
        })?;
        let downstream_node = self.stream_nodes.get(pump.downstream()).ok_or_else(|| {
            SpringError::Sql(anyhow!(
                r#"downstream "{}" does not exist in pipeline"#,
                pump.downstream()
            ))
        })?;

        let _ = self
            .graph
            .add_edge(*upstream_node, *downstream_node, PumpEdge::Pump(pump));

        Ok(())
    }

    pub(super) fn add_server(&mut self, server: ServerModel) -> Result<()> {
        let serving_to = server.serving_foreign_stream();

        match server.server_type() {
            ServerType::SourceNet => {
                let upstream_node = self
                    .stream_nodes
                    .get(&StreamName::virtual_root())
                    .expect("virtual root always available");
                let downstream_node = self.stream_nodes.get(serving_to).ok_or_else(|| {
                    SpringError::Sql(anyhow!(
                        r#"downstream "{}" does not exist in pipeline"#,
                        serving_to
                    ))
                })?;
                let _ =
                    self.graph
                        .add_edge(*upstream_node, *downstream_node, PumpEdge::Source(server));
            }
        }
        Ok(())
    }
}
