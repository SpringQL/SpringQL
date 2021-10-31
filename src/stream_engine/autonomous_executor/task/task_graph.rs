//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! A TaskGraph has "virtual root stream", who has outgoing edges to all source foreign streams.
//!
//! Tasks are mapped to TaskGraph's edges. StreamName's are mapped to its nodes.

use petgraph::graph::DiGraph;

use crate::{model::name::StreamName, stream_engine::pipeline::pipeline_graph::PipelineGraph};

use super::Task;

#[derive(Debug)]
pub(in crate::stream_engine) struct TaskGraph(DiGraph<StreamName, Task>);

impl From<&PipelineGraph> for TaskGraph {
    fn from(p_graph: &PipelineGraph) -> Self {
        todo!()
    }
}
