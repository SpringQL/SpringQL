// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expr_resolver::expr_label::ExprLabel,
    pipeline::{
        name::StreamName,
        pump_model::{
            WindowParameter, {JoinParameter, WindowOperationParameter},
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
    pub(crate) expr_labels: Vec<ExprLabel>,
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
