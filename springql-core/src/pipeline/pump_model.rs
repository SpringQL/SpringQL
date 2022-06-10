// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod pump_input_type;
mod window_operation_parameter;
mod window_parameter;

pub use pump_input_type::PumpInputType;
pub use window_operation_parameter::{
    AggregateFunctionParameter, AggregateParameter, GroupByLabels, JoinParameter, JoinType,
    WindowOperationParameter,
};
pub use window_parameter::WindowParameter;

use crate::{
    pipeline::name::{PumpName, StreamName},
    stream_engine::command::{insert_plan::InsertPlan, query_plan::QueryPlan},
};

#[derive(Clone, PartialEq, Debug, new)]
pub struct PumpModel {
    name: PumpName,
    query_plan: QueryPlan,
    insert_plan: InsertPlan,
}

impl PumpModel {
    pub fn name(&self) -> &PumpName {
        &self.name
    }

    pub fn input_type(&self) -> PumpInputType {
        self.query_plan.input_type()
    }

    pub fn query_plan(&self) -> &QueryPlan {
        &self.query_plan
    }

    pub fn insert_plan(&self) -> &InsertPlan {
        &self.insert_plan
    }

    /// Has more than 1 upstreams on JOIN, for example.
    pub fn upstreams(&self) -> Vec<&StreamName> {
        self.query_plan.upstreams()
    }

    pub fn downstream(&self) -> &StreamName {
        self.insert_plan.stream()
    }
}
