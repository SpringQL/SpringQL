use petgraph::graph::{EdgeIndex, NodeIndex};

use super::queue_id::QueueId;

/// Original [EdgeReference](https://docs.rs/petgraph/0.6.0/petgraph/graph/struct.EdgeReference.html) is
/// only constructed via [edge_references()](https://docs.rs/petgraph/0.6.0/petgraph/graph/struct.Graph.html#method.edge_references)
/// traversal.
#[derive(Clone, Eq, PartialEq, Hash, Debug, new)]
pub(super) struct MyEdgeRef {
    source: NodeIndex,
    target: NodeIndex,
    edge: EdgeIndex,
    weight: QueueId,
}

impl MyEdgeRef {
    pub(super) fn source(&self) -> NodeIndex {
        self.source
    }

    pub(super) fn target(&self) -> NodeIndex {
        self.target
    }

    pub(super) fn edge(&self) -> EdgeIndex {
        self.edge
    }

    pub(super) fn weight(&self) -> &QueueId {
        &self.weight
    }
}
