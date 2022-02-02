pub(crate) mod comparison_function;
pub(crate) mod logical_function;
pub(crate) mod numerical_function;

use self::{
    comparison_function::ComparisonFunction, logical_function::LogicalFunction,
    numerical_function::NumericalFunction,
};

/// Boolean expression.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum BooleanExpression {
    /// AND, OR, NOT
    LogicalFunctionVariant(LogicalFunction),

    /// Comparison functions
    ComparisonFunctionVariant(ComparisonFunction),

    NumericalFunctionVariant(NumericalFunction),
}
