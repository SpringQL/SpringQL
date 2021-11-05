use serde::{Deserialize, Serialize};

use self::operation::LeafOperation;

pub(crate) mod operation;

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum QueryPlanNode {
    Leaf(QueryPlanNodeLeaf),
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct QueryPlanNodeLeaf {
    pub(crate) op: LeafOperation,
}
