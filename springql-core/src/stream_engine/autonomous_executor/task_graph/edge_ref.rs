// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use petgraph::graph::NodeIndex;

/// Original `EdgeReference` is
/// only constructed via `edge_references()`
/// traversal.
#[derive(Clone, Eq, PartialEq, Hash, Debug, new)]
pub struct MyEdgeRef {
    source: NodeIndex,
    target: NodeIndex,
}

impl MyEdgeRef {
    pub fn source(&self) -> NodeIndex {
        self.source
    }

    pub fn target(&self) -> NodeIndex {
        self.target
    }
}
