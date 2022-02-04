// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expression::{
        boolean_expression::{
            comparison_function::ComparisonFunction, logical_function::LogicalFunction,
            BooleanExpression,
        },
        operator::UnaryOperator,
        ValueExpr,
    },
    pipeline::{
        field::field_name::ColumnReference,
        name::{AttributeName, ColumnName, CorrelationAlias, PumpName, StreamName},
    },
    stream_engine::SqlValue,
};

impl StreamName {
    pub(crate) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl PumpName {
    pub(crate) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl ValueExpr {
    pub fn factory_null() -> Self {
        Self::Constant(SqlValue::Null)
    }

    pub fn factory_integer(integer: i32) -> Self {
        Self::Constant(SqlValue::factory_integer(integer))
    }

    pub fn factory_uni_op(unary_operator: UnaryOperator, expression: ValueExpr) -> Self {
        Self::UnaryOperator(unary_operator, Box::new(expression))
    }

    pub fn factory_eq(left: ValueExpr, right: ValueExpr) -> Self {
        Self::BooleanExpr(BooleanExpression::factory_eq(left, right))
    }

    pub fn factory_and(left: BooleanExpression, right: BooleanExpression) -> Self {
        Self::BooleanExpr(BooleanExpression::LogicalFunctionVariant(
            LogicalFunction::AndVariant {
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }
}

impl BooleanExpression {
    pub fn factory_eq(left: ValueExpr, right: ValueExpr) -> Self {
        BooleanExpression::ComparisonFunctionVariant(ComparisonFunction::EqualVariant {
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}

impl ColumnReference {
    pub(crate) fn factory(stream_name: &str, column_name: &str) -> Self {
        Self::new(
            StreamName::factory(stream_name),
            ColumnName::new(column_name.to_string()),
        )
    }
}

impl CorrelationAlias {
    pub(crate) fn factory(correlation_alias: &str) -> Self {
        Self::new(correlation_alias.to_string())
    }
}

impl AttributeName {
    pub(crate) fn factory(column_name: &str) -> Self {
        Self::new(column_name.to_string())
    }
}
