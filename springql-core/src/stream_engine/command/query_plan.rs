// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod query_plan_operation;

pub use query_plan_operation::{
    CollectOp, GroupAggregateWindowOp, JoinOp, JoinWindowOp, LowerOps, ProjectionOp, UpperOps,
};

use crate::{
    expr_resolver::ExprResolver,
    pipeline::{PumpInputType, StreamName},
};

/// Query plan from which an executor can do its work deterministically.
#[derive(Clone, PartialEq, Debug, new)]
pub struct QueryPlan {
    pub upper_ops: UpperOps,
    pub lower_ops: LowerOps,

    /// to convert *Expr in *Syntax into *ExprLabel
    pub expr_resolver: ExprResolver,
}

impl QueryPlan {
    pub fn input_type(&self) -> PumpInputType {
        if self.upper_ops.has_window() || self.lower_ops.has_window() {
            PumpInputType::Window
        } else {
            PumpInputType::Row
        }
    }

    pub fn upstreams(&self) -> Vec<&StreamName> {
        match &self.lower_ops.join {
            JoinOp::Collect(collect) => vec![&collect.stream],
            JoinOp::JoinWindow(JoinWindowOp { left, right, .. }) => {
                vec![&left.stream, &right.stream]
            }
        }
    }
}
