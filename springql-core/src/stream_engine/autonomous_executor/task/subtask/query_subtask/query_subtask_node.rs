// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

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
    #[allow(dead_code)]
    SlidingWindow(SlidingWindowSubtask),
}

impl From<&QueryPlanOperation> for QuerySubtaskNode {
    fn from(op: &QueryPlanOperation) -> Self {
        match op {
            QueryPlanOperation::Collect { .. } => QuerySubtaskNode::Collect(CollectSubtask::new()),
            QueryPlanOperation::Projection { column_names } => {
                QuerySubtaskNode::Projection(ProjectionSubtask::new(column_names.to_vec()))
            }
            QueryPlanOperation::TimeBasedSlidingWindow { .. } => {
                todo!()
                // QuerySubtaskNode::SlidingWindow(SlidingWindowSubtask::register(
                //     chrono::Duration::from_std(*lower_bound)
                //         .expect("std::Duration -> chrono::Duration"),
                // ))
            }
        }
    }
}
