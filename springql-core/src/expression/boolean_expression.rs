// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod comparison_function;
mod logical_function;
mod numerical_function;

pub(crate) use comparison_function::ComparisonFunction;
pub(crate) use logical_function::LogicalFunction;
pub(crate) use numerical_function::NumericalFunction;

use crate::expression::ValueExprType;

/// Boolean expression.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Hash, Debug)]
pub enum BinaryExpr<E>
where
    E: ValueExprType,
{
    /// AND, OR, NOT
    LogicalFunctionVariant(LogicalFunction<E>),

    /// Comparison functions
    ComparisonFunctionVariant(ComparisonFunction<E>),

    NumericalFunctionVariant(NumericalFunction<E>),
}
