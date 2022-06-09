// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod aggregate;
pub(crate) mod join_parameter;

use crate::pipeline::pump_model::window_operation_parameter::join_parameter::JoinParameter;
pub(crate) use aggregate::{AggregateFunctionParameter, AggregateParameter, GroupByLabels};

/// Window operation parameters
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    Aggregate(AggregateParameter),
    Join(JoinParameter),
}
