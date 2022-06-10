// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expression::{AggrExpr, ValueExpr},
    pipeline::{
        pump_model::{JoinType, WindowParameter},
        AggrAlias, CorrelationAlias, StreamName, ValueAlias,
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
pub struct SelectStreamSyntax {
    pub(in crate::sql_processor) fields: Vec<SelectFieldSyntax>,
    pub(in crate::sql_processor) from_item: FromItemSyntax,

    /// Empty when no GROUP BY clause is supplied.
    pub(in crate::sql_processor) grouping_elements: Vec<GroupingElementSyntax>,

    pub(in crate::sql_processor) window_clause: Option<WindowParameter>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SelectFieldSyntax {
    ValueExpr {
        value_expr: ValueExpr,
        alias: Option<ValueAlias>,
    },
    AggrExpr {
        aggr_expr: AggrExpr,
        alias: Option<AggrAlias>,
    },
}

#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) enum FromItemSyntax {
    StreamVariant(SubFromItemSyntax),
    JoinVariant {
        left: SubFromItemSyntax,
        right: Box<FromItemSyntax>,

        join_type: JoinType,
        on_expr: ValueExpr,
        // TODO alias
    },
}

#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) struct SubFromItemSyntax {
    pub(in crate::sql_processor) stream_name: StreamName,
    pub(in crate::sql_processor) alias: Option<CorrelationAlias>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum GroupingElementSyntax {
    ValueExpr(ValueExpr),
    ValueAlias(ValueAlias),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum DurationFunction {
    Millis,
    Secs,
}
