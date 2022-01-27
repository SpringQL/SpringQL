// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expression::Expression,
    pipeline::{
        field::field_pointer::FieldPointer,
        name::{CorrelationAlias, FieldAlias, StreamName},
        pump_model::window_operation_parameter::{AggregateFunctionParameter, AggregateParameter},
    },
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum ColumnConstraintSyntax {
    NotNull, // this is treated as data type in pipeline
    Rowtime,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) struct OptionSyntax {
    pub(in crate::sql_processor) option_name: String,
    pub(in crate::sql_processor) option_value: String,
}

#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) struct SelectStreamSyntax {
    pub(in crate::sql_processor) fields: Vec<SelectFieldSyntax>,
    pub(in crate::sql_processor) from_item: FromItemSyntax,
}

#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) enum SelectFieldSyntax {
    Expression {
        expression: Expression,
        alias: Option<FieldAlias>,
    },
    Aggregate(AggregateParameter),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum FromItemSyntax {
    StreamVariant {
        stream_name: StreamName,
        alias: Option<CorrelationAlias>,
    },
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) struct AggregateSyntax {
    pub(in crate::sql_processor) aggregate_function: AggregateFunctionParameter,
    pub(in crate::sql_processor) aggregated: FieldPointer,
}
