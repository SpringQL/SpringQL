// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expression::ValueExprPh1,
    pipeline::name::{CorrelationAlias, FieldAlias, StreamName},
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
pub(in crate::sql_processor) struct SelectFieldSyntax {
    pub(in crate::sql_processor) value_expr: ValueExprPh1,
    pub(in crate::sql_processor) alias: Option<FieldAlias>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum FromItemSyntax {
    StreamVariant {
        stream_name: StreamName,
        alias: Option<CorrelationAlias>,
    },
}
