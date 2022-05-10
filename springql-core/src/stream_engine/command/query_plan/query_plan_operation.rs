// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expr_resolver::expr_label::{AggrExprLabel, ValueExprLabel},
    pipeline::{
        name::StreamName,
        pump_model::{
            window_operation_parameter::{
                aggregate::GroupByLabels, join_parameter::JoinParameter, WindowOperationParameter,
            },
            window_parameter::WindowParameter,
        },
    },
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct UpperOps {
    pub(crate) projection: ProjectionOp,
    pub(crate) group_aggr_window: Option<GroupAggregateWindowOp>,
}
impl UpperOps {
    pub(crate) fn has_window(&self) -> bool {
        self.group_aggr_window.is_some()
    }

    pub(crate) fn group_by_labels(&self) -> GroupByLabels {
        self.group_aggr_window
            .as_ref()
            .and_then(|group_aggr_window_op| {
                if let WindowOperationParameter::Aggregate(aggregate_parameter) =
                    &group_aggr_window_op.op_param
                {
                    Some(aggregate_parameter.group_by.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct LowerOps {
    pub(crate) join: JoinOp,
}
impl LowerOps {
    pub(crate) fn has_window(&self) -> bool {
        matches!(self.join, JoinOp::JoinWindow(_))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ProjectionOp {
    pub(crate) value_expr_labels: Vec<ValueExprLabel>,
    pub(crate) aggr_expr_labels: Vec<AggrExprLabel>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct GroupAggregateWindowOp {
    pub(crate) window_param: WindowParameter,
    pub(crate) op_param: WindowOperationParameter,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct CollectOp {
    pub(crate) stream: StreamName,
}

/// TODO recursive join
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum JoinOp {
    Collect(CollectOp),
    JoinWindow(JoinWindowOp),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct JoinWindowOp {
    pub(crate) left: CollectOp,
    pub(crate) right: CollectOp,

    pub(crate) window_param: WindowParameter,
    pub(crate) join_param: JoinParameter,
}
