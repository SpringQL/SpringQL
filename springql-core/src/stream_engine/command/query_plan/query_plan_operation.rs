// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::time::Duration;

use crate::{
    expr_resolver::expr_label::ExprLabel,
    expression::ValueExpr,
    pipeline::{field::field_name::ColumnReference, name::StreamName},
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct UpperOps {
    pub(crate) projection: ProjectionOp,
    pub(crate) eval_value_expr: EvalValueExprOp,
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
pub(crate) struct EvalValueExprOp {
    pub(crate) expressions: Vec<ValueExpr>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct CollectOp {
    pub(crate) stream: StreamName,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum QueryPlanOperation {
    TimeBasedSlidingWindow {
        /// cannot use chrono::Duration here: <https://github.com/chronotope/chrono/issues/117>
        lower_bound: Duration,
    },
}
