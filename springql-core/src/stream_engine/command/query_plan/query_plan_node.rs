use binary_tree::Node;
use serde::{Deserialize, Serialize};

use self::operation::QueryPlanOperation;

pub(crate) mod operation;

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct QueryPlanNode {
    op: QueryPlanOperation,
    left: Option<Box<Self>>,
    right: Option<Box<Self>>,
}

impl Node for QueryPlanNode {
    type Value = QueryPlanOperation;

    fn left(&self) -> Option<&Self> {
        self.left.as_ref().map(|b| b.as_ref())
    }

    fn right(&self) -> Option<&Self> {
        self.right.as_ref().map(|b| b.as_ref())
    }

    fn value(&self) -> &Self::Value {
        &self.op
    }
}

impl QueryPlanNode {
    pub(crate) fn new(op: QueryPlanOperation, left: Option<Self>, right: Option<Self>) -> Self {
        Self {
            op,
            left: left.map(Box::new),
            right: right.map(Box::new),
        }
    }

}
