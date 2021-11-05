pub(super) mod collect_subtask;
pub(super) mod window_subtask;

use std::fmt::Debug;

use crate::stream_engine::command::query_plan::query_plan_node::{
    operation::LeafOperation, QueryPlanNode,
};

use self::{collect_subtask::CollectSubtask, window_subtask::SlidingWindowSubtask};

#[derive(Debug)]
pub(super) enum QuerySubtaskNode {
    Collect(CollectSubtask),
    Stream(StreamSubtask),
    Window(WindowSubtask),
}

impl From<&QueryPlanNode> for QuerySubtaskNode {
    fn from(node: &QueryPlanNode) -> Self {
        match node {
            QueryPlanNode::Leaf(leaf_node) => match &leaf_node.op {
                LeafOperation::Collect => QuerySubtaskNode::Collect(CollectSubtask::new()),
            },
        }
    }
}

#[derive(Debug)]
pub(super) enum StreamSubtask {}

#[derive(Debug)]
pub(super) enum WindowSubtask {
    Sliding(SlidingWindowSubtask),
}
