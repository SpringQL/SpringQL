// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expr_resolver::expr_label::{ValueExprLabel, AggrExprLabel},
    pipeline::{
        name::StreamName,
        pump_model::{
            window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
        },
    },
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct UpperOps {
    pub(crate) projection: ProjectionOp,
    pub(crate) group_aggr_window: Option<GroupAggregateWindowOp>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct LowerOps {
    pub(crate) collect: CollectOp,
    // TODO join
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ProjectionOp {
    pub(crate) value_expr_labels: Vec<ValueExprLabel>,
    pub(crate) aggr_expr_labels: Vec<AggrExprLabel>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct CollectOp {
    pub(crate) stream: StreamName,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct GroupAggregateWindowOp {
    pub(crate) window_param: WindowParameter,
    pub(crate) op_param: WindowOperationParameter,
}
