// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod pump_input_type;
pub(crate) mod window_operation_parameter;
pub(crate) mod window_parameter;

use crate::stream_engine::command::{insert_plan::InsertPlan, query_plan::QueryPlan};

use self::pump_input_type::PumpInputType;

use super::name::{PumpName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct PumpModel {
    name: PumpName,
    query_plan: QueryPlan,
    insert_plan: InsertPlan,
}

impl PumpModel {
    pub(crate) fn name(&self) -> &PumpName {
        &self.name
    }

    pub(crate) fn input_type(&self) -> PumpInputType {
        self.query_plan.input_type()
    }

    pub(crate) fn query_plan(&self) -> &QueryPlan {
        &self.query_plan
    }

    pub(crate) fn insert_plan(&self) -> &InsertPlan {
        &self.insert_plan
    }

    /// Has more than 1 upstreams on JOIN, for example.
    pub(crate) fn upstreams(&self) -> Vec<&StreamName> {
        self.query_plan.upstreams()
    }

    pub(crate) fn downstream(&self) -> &StreamName {
        self.insert_plan.stream()
    }
}
