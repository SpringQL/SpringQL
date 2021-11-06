use binary_tree::Node;

use self::operation::QueryPlanOperation;

pub(crate) mod operation;

#[derive(Eq, PartialEq, Clone, Debug)]
pub(crate) struct QueryPlanNode {
    op: QueryPlanOperation,
    left: Option<Box<Self>>,
    right: Option<Box<Self>>,
}

impl Node for QueryPlanNode {
    type Value = QueryPlanOperation;

    fn left(&self) -> Option<&Self> {
        self.left.map(|b| b.as_ref())
    }

    fn right(&self) -> Option<&Self> {
        self.right.map(|b| b.as_ref())
    }

    fn value(&self) -> &Self::Value {
        &self.op
    }
}
