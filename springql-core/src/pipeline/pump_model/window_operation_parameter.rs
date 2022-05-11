// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod aggregate;
pub(crate) mod join_parameter;

use self::{aggregate::AggregateParameter, join_parameter::JoinParameter};

/// Window operation parameters
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    Aggregate(AggregateParameter),
    Join(JoinParameter),
}
