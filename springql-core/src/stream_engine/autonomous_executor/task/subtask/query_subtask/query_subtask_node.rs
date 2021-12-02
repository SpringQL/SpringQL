pub(super) mod collect_subtask;
pub(super) mod projection_subtask;
pub(super) mod window_subtask;

use std::fmt::Debug;

use crate::stream_engine::command::query_plan::query_plan_operation::QueryPlanOperation;

use self::{
    collect_subtask::CollectSubtask, projection_subtask::ProjectionSubtask,
    window_subtask::SlidingWindowSubtask,
};

#[derive(Debug)]
pub(super) enum QuerySubtaskNode {
    Collect(CollectSubtask),
    Projection(ProjectionSubtask),
    SlidingWindow(SlidingWindowSubtask),
}

impl From<&QueryPlanOperation> for QuerySubtaskNode {
    fn from(op: &QueryPlanOperation) -> Self {
        match op {
            QueryPlanOperation::Collect { .. } => QuerySubtaskNode::Collect(CollectSubtask::new()),
            QueryPlanOperation::Projection { column_names } => {
                QuerySubtaskNode::Projection(ProjectionSubtask::new(column_names.to_vec()))
            }
            QueryPlanOperation::TimeBasedSlidingWindow { lower_bound } => {
                QuerySubtaskNode::SlidingWindow(SlidingWindowSubtask::register(
                    chrono::Duration::from_std(*lower_bound)
                        .expect("std::Duration -> chrono::Duration"),
                ))
            }
        }
    }
}
