// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use petgraph::graph::NodeIndex;

/// Original [EdgeReference](https://docs.rs/petgraph/0.6.0/petgraph/graph/struct.EdgeReference.html) is
/// only constructed via [edge_references()](https://docs.rs/petgraph/0.6.0/petgraph/graph/struct.Graph.html#method.edge_references)
/// traversal.
#[derive(Clone, Eq, PartialEq, Hash, Debug, new)]
pub(super) struct MyEdgeRef {
    source: NodeIndex,
    target: NodeIndex,
}

impl MyEdgeRef {
    pub(super) fn source(&self) -> NodeIndex {
        self.source
    }

    pub(super) fn target(&self) -> NodeIndex {
        self.target
    }
}
