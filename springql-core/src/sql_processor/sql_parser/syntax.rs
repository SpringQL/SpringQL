// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expression::{AggrExpr, ValueExpr},
    pipeline::{AggrAlias, CorrelationAlias, JoinType, StreamName, ValueAlias, WindowParameter},
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ColumnConstraintSyntax {
    NotNull, // this is treated as data type in pipeline
    Rowtime,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OptionSyntax {
    pub option_name: String,
    pub option_value: String,
}

#[derive(Clone, PartialEq, Debug)]
pub struct SelectStreamSyntax {
    pub fields: Vec<SelectFieldSyntax>,
    pub from_item: FromItemSyntax,

    /// Empty when no GROUP BY clause is supplied.
    pub grouping_elements: Vec<GroupingElementSyntax>,

    pub window_clause: Option<WindowParameter>,
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
pub enum FromItemSyntax {
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
pub struct SubFromItemSyntax {
    pub stream_name: StreamName,
    pub alias: Option<CorrelationAlias>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum GroupingElementSyntax {
    ValueExpr(ValueExpr),
    ValueAlias(ValueAlias),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum DurationFunction {
    Millis,
    Secs,
}
