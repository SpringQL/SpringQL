// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expr_resolver::expr_label::ExprLabel,
    expression::ValueExpr,
    pipeline::{
        name::{AggrAlias, CorrelationAlias, StreamName, ValueAlias},
        pump_model::{
            window_operation_parameter::aggregate::{
                AggregateFunctionParameter, AggregateParameter,
            },
            window_parameter::WindowParameter,
        },
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
    pub(in crate::sql_processor) grouping_element: Option<ExprLabel>,
    pub(in crate::sql_processor) window_clause: Option<WindowParameter>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum SelectFieldSyntax {
    ValueExpr {
        value_expr: ValueExpr,
        alias: Option<ValueAlias>,
    },
    AggrExpr {
        aggr_expr: AggregateParameter,
        alias: Option<AggrAlias>,
    },
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
    pub(in crate::sql_processor) aggregated: ExprLabel,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum DurationFunction {
    Secs,
}
