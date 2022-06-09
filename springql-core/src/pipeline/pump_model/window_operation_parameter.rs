// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod aggregate;
mod join_parameter;

pub(crate) use aggregate::{AggregateFunctionParameter, AggregateParameter, GroupByLabels};
pub(crate) use join_parameter::{JoinParameter, JoinType};

/// Window operation parameters
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    Aggregate(AggregateParameter),
    Join(JoinParameter),
}
