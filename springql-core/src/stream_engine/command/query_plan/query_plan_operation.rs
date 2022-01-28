// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::{
    field::field_pointer::FieldPointer,
    name::StreamName,
    pump_model::{
        window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
    },
};

use super::aliaser::Aliaser;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum QueryPlanOperation {
    Collect {
        stream: StreamName,
        aliaser: Aliaser,
    },
    Projection {
        field_pointers: Vec<FieldPointer>,
    },
    GroupAggregateWindow {
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    },
}
