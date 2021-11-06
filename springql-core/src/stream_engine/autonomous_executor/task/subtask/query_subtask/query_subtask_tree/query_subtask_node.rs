pub(super) mod collect_subtask;
pub(super) mod window_subtask;

use std::fmt::Debug;

use crate::stream_engine::command::query_plan::query_plan_operation::QueryPlanOperation;

use self::{collect_subtask::CollectSubtask, window_subtask::SlidingWindowSubtask};

#[derive(Debug)]
pub(super) enum QuerySubtaskNode {
    Collect(CollectSubtask),
    Stream(StreamSubtask),
    Window(WindowSubtask),
}

impl From<&QueryPlanOperation> for QuerySubtaskNode {
    fn from(op: &QueryPlanOperation) -> Self {
        match op {
            QueryPlanOperation::Collect { stream } => {
                QuerySubtaskNode::Collect(CollectSubtask::new())
            }
            QueryPlanOperation::TimeBasedSlidingWindow { lower_bound } => {
                QuerySubtaskNode::Window(WindowSubtask::Sliding(SlidingWindowSubtask::register(
                    chrono::Duration::from_std(*lower_bound)
                        .expect("std::Duration -> chrono::Duration"),
                )))
            }
        }
    }
}

#[derive(Debug)]
pub(super) enum StreamSubtask {}

#[derive(Debug)]
pub(super) enum WindowSubtask {
    Sliding(SlidingWindowSubtask),
}
