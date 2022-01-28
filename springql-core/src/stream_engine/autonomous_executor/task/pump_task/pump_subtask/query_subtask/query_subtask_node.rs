// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod collect_subtask;
pub(super) mod group_aggregate_window_subtask;
pub(super) mod projection_subtask;

use std::fmt::Debug;

use crate::stream_engine::command::query_plan::query_plan_operation::QueryPlanOperation;

use self::{
    collect_subtask::CollectSubtask, group_aggregate_window_subtask::GroupAggregateWindowSubtask,
    projection_subtask::ProjectionSubtask,
};

#[derive(Debug)]
pub(super) enum QuerySubtaskNode {
    Collect(CollectSubtask),
    Projection(ProjectionSubtask),
    GroupAggregateWindow(Box<GroupAggregateWindowSubtask>), // Boxed to avoid <https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant>
}

impl From<&QueryPlanOperation> for QuerySubtaskNode {
    fn from(op: &QueryPlanOperation) -> Self {
        match op {
            QueryPlanOperation::Collect { aliaser, .. } => {
                QuerySubtaskNode::Collect(CollectSubtask::new(aliaser.clone()))
            }
            QueryPlanOperation::Projection { field_pointers } => {
                QuerySubtaskNode::Projection(ProjectionSubtask::new(field_pointers.to_vec()))
            }
            QueryPlanOperation::GroupAggregateWindow {
                window_param,
                op_param,
            } => QuerySubtaskNode::GroupAggregateWindow(Box::new(
                GroupAggregateWindowSubtask::new(window_param.clone(), op_param.clone()),
            )),
        }
    }
}
