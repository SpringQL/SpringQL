use binary_tree::BinaryTree;
use serde::{Deserialize, Serialize};

use crate::pipeline::name::StreamName;

use self::query_plan_node::QueryPlanNode;

pub(crate) mod query_plan_node;

/// Query plan from which an executor can do its work deterministically.
///
/// This is a binary tree because every SELECT operation can break down into unary or binary operations.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct QueryPlan {
    root: QueryPlanNode,
}

impl BinaryTree for QueryPlan {
    type Node = QueryPlanNode;

    fn root(&self) -> Option<&Self::Node> {
        Some(&self.root)
    }
}

impl QueryPlan {
    pub(crate) fn upstreams(&self) -> &[StreamName] {
        todo!()
    }
}
