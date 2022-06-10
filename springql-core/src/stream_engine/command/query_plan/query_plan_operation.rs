// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expr_resolver::expr_label::ExprLabel,
    pipeline::{JoinParameter, StreamName, WindowOperationParameter, WindowParameter},
};

#[derive(Clone, PartialEq, Debug)]
pub struct UpperOps {
    pub projection: ProjectionOp,
    pub group_aggr_window: Option<GroupAggregateWindowOp>,
}
impl UpperOps {
    pub fn has_window(&self) -> bool {
        self.group_aggr_window.is_some()
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct LowerOps {
    pub join: JoinOp,
}
impl LowerOps {
    pub fn has_window(&self) -> bool {
        matches!(self.join, JoinOp::JoinWindow(_))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ProjectionOp {
    pub expr_labels: Vec<ExprLabel>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct GroupAggregateWindowOp {
    pub window_param: WindowParameter,
    pub op_param: WindowOperationParameter,
}

#[derive(Clone, PartialEq, Debug)]
pub struct CollectOp {
    pub stream: StreamName,
}

/// TODO recursive join
#[derive(Clone, PartialEq, Debug)]
pub enum JoinOp {
    Collect(CollectOp),
    JoinWindow(JoinWindowOp),
}

#[derive(Clone, PartialEq, Debug)]
pub struct JoinWindowOp {
    pub left: CollectOp,
    pub right: CollectOp,

    pub window_param: WindowParameter,
    pub join_param: JoinParameter,
}
