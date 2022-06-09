// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expression::{
        BinaryExpr, ComparisonFunction, LogicalFunction, NumericalFunction, UnaryOperator,
        ValueExpr, ValueExprType,
    },
    pipeline::{
        field::ColumnReference,
        name::{ColumnName, StreamName},
    },
    stream_engine::SqlValue,
};

impl StreamName {
    pub(crate) fn factory(name: &str) -> Self {
        Self::new(name.to_string())
    }
}

impl ValueExpr {
    pub fn factory_null() -> Self {
        Self::Constant(SqlValue::Null)
    }

    pub fn factory_colref(stream_name: &str, column_name: &str) -> Self {
        let colref = ColumnReference::factory(stream_name, column_name);
        Self::ColumnReference(colref)
    }

    pub fn factory_integer(integer: i32) -> Self {
        Self::Constant(SqlValue::factory_integer(integer))
    }

    pub fn factory_uni_op(unary_operator: UnaryOperator, expression: ValueExpr) -> Self {
        Self::UnaryOperator(unary_operator, Box::new(expression))
    }

    pub fn factory_eq(left: ValueExpr, right: ValueExpr) -> Self {
        Self::BinaryExpr(BinaryExpr::factory_eq(left, right))
    }

    pub fn factory_add(left: ValueExpr, right: ValueExpr) -> Self {
        Self::BinaryExpr(BinaryExpr::factory_add(left, right))
    }

    pub fn factory_and(left: ValueExpr, right: ValueExpr) -> Self {
        Self::BinaryExpr(BinaryExpr::LogicalFunctionVariant(
            LogicalFunction::AndVariant {
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }
}

impl<E: ValueExprType> BinaryExpr<E> {
    pub fn factory_eq(left: E, right: E) -> Self {
        BinaryExpr::ComparisonFunctionVariant(ComparisonFunction::EqualVariant {
            left: Box::new(left),
            right: Box::new(right),
        })
    }
    pub fn factory_add(left: E, right: E) -> Self {
        BinaryExpr::NumericalFunctionVariant(NumericalFunction::AddVariant {
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
