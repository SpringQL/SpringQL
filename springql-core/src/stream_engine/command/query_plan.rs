// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod query_plan_operation;

use crate::{
    expr_resolver::ExprResolver,
    pipeline::{pump_model::PumpInputType, StreamName},
    stream_engine::command::query_plan::query_plan_operation::{
        JoinOp, JoinWindowOp, LowerOps, UpperOps,
    },
};

/// Query plan from which an executor can do its work deterministically.
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct QueryPlan {
    pub(crate) upper_ops: UpperOps,
    pub(crate) lower_ops: LowerOps,

    /// to convert *Expr in *Syntax into *ExprLabel
    pub(crate) expr_resolver: ExprResolver,
}

impl QueryPlan {
    pub(crate) fn input_type(&self) -> PumpInputType {
        if self.upper_ops.has_window() || self.lower_ops.has_window() {
            PumpInputType::Window
        } else {
            PumpInputType::Row
        }
    }

    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        match &self.lower_ops.join {
            JoinOp::Collect(collect) => vec![&collect.stream],
            JoinOp::JoinWindow(JoinWindowOp { left, right, .. }) => {
                vec![&left.stream, &right.stream]
            }
        }
    }
}
