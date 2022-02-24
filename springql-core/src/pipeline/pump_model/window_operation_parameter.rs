// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod aggregate;
pub(crate) mod join_parameter;

use self::{aggregate::GroupAggregateParameter, join_parameter::JoinParameter};

/// Window operation parameters
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    GroupAggregation(GroupAggregateParameter),
    Join(JoinParameter),
}
