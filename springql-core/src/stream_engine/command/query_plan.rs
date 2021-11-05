use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::query_plan_node::QueryPlanNode;

pub(crate) mod query_plan_node;

/// Query plan from which an executor can do its work deterministically.
///
/// This is a binary tree because every SELECT operation can break down into unary or binary operations.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct QueryPlan {
    root: Arc<QueryPlanNode>,
}

impl QueryPlan {
    pub(crate) fn root(&self) -> Arc<QueryPlanNode> {
        self.root.clone()
    }
}
