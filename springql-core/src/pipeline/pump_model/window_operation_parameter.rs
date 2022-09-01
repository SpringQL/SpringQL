// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod aggregate;
mod join_parameter;

pub use aggregate::{AggregateFunctionParameter, AggregateParameter, GroupByLabels};
pub use join_parameter::{JoinParameter, JoinType};

/// Window operation parameters
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WindowOperationParameter {
    Aggregate(AggregateParameter),
    Join(JoinParameter),
}
