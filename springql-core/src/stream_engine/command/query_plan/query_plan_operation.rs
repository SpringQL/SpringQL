// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expr_resolver::expr_label::ExprLabel,
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
    // TODO option group_aggregate
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct LowerOps {
    pub(crate) collect: CollectOp,
    // TODO join
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ProjectionOp {
    pub(crate) expr_labels: Vec<ExprLabel>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct CollectOp {
    pub(crate) stream: StreamName,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct GroupAggregateWindowOp {
    window_param: WindowParameter,
    op_param: WindowOperationParameter,
}
