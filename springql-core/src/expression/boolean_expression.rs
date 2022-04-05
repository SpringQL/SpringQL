// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod comparison_function;
pub(crate) mod logical_function;
pub(crate) mod numerical_function;

use self::{
    comparison_function::ComparisonFunction, logical_function::LogicalFunction,
    numerical_function::NumericalFunction,
};

use super::ValueExprType;

/// Boolean expression.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum BinaryExpr<E>
where
    E: ValueExprType,
{
    /// AND, OR, NOT
    LogicalFunctionVariant(LogicalFunction<E>),

    /// Comparison functions
    ComparisonFunctionVariant(ComparisonFunction<E>),

    NumericalFunctionVariant(NumericalFunction<E>),
}
