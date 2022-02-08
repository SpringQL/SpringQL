// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod query_plan_operation;

pub(in crate::stream_engine) mod child_direction;

use crate::pipeline::{name::StreamName, pump_model::pump_input_type::PumpInputType};

use self::query_plan_operation::{LowerOps, UpperOps};

/// Query plan from which an executor can do its work deterministically.
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct QueryPlan {
    pub(crate) upper_ops: UpperOps,
    pub(crate) lower_ops: LowerOps,
}

impl QueryPlan {
    pub(crate) fn input_type(&self) -> PumpInputType {
        // TODO distinguish window input
        PumpInputType::Row
    }

    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        let stream = &self.lower_ops.collect.stream;
        vec![stream]
    }
}
