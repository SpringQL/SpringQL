// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expression::{
        boolean_expression::{
            comparison_function::ComparisonFunction, logical_function::LogicalFunction,
            BooleanExpression,
        },
        operator::UnaryOperator,
        Expression,
    },
    pipeline::{
        correlation::aliased_correlation_name::AliasedCorrelationName,
        field::{aliased_field_name::AliasedFieldName, field_name::FieldName},
        name::{
            AttributeName, CorrelationAlias, CorrelationName, FieldAlias, PumpName, StreamName,
        },
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

impl AliasedFieldName {
    pub(crate) fn factory(stream_name: &str, column_name: &str) -> Self {
        Self::new(FieldName::factory(stream_name, column_name), None)
    }

    pub(crate) fn with_corr_alias(self, correlation_alias: &str) -> Self {
        let field_name = self.field_name.with_corr_alias(correlation_alias);
        Self::new(field_name, None)
    }

    pub(crate) fn with_field_alias(self, field_alias: &str) -> Self {
        let alias = FieldAlias::new(field_alias.to_string());
        Self::new(self.field_name, Some(alias))
    }
}

impl Expression {
    pub fn factory_null() -> Self {
        Self::Constant(SqlValue::Null)
    }

    pub fn factory_integer(integer: i32) -> Self {
        Self::Constant(SqlValue::factory_integer(integer))
    }

    pub fn factory_uni_op(unary_operator: UnaryOperator, expression: Expression) -> Self {
        Self::UnaryOperator(unary_operator, Box::new(expression))
    }

    pub fn factory_eq(left: Expression, right: Expression) -> Self {
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
    pub fn factory_eq(left: Expression, right: Expression) -> Self {
        BooleanExpression::ComparisonFunctionVariant(ComparisonFunction::EqualVariant {
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}

impl FieldName {
    pub(crate) fn factory(stream_name: &str, column_name: &str) -> Self {
        Self::new(
            AliasedCorrelationName::factory_sn(stream_name),
            AttributeName::factory(column_name),
        )
    }

    pub(crate) fn with_corr_alias(self, correlation_alias: &str) -> Self {
        let aliased_correlation_name = self.aliased_correlation_name.with_alias(correlation_alias);
        Self::new(aliased_correlation_name, self.attribute_name)
    }
}

impl AliasedCorrelationName {
    pub(crate) fn factory_sn(stream_name: &str) -> Self {
        Self::new(CorrelationName::factory(stream_name), None)
    }

    pub(crate) fn with_alias(self, correlation_alias: &str) -> Self {
        let alias = CorrelationAlias::factory(correlation_alias);
        Self::new(self.correlation_name, Some(alias))
    }
}

impl CorrelationName {
    pub(crate) fn factory(stream_name: &str) -> Self {
        Self::new(stream_name.to_string())
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
