pub(crate) mod comparison_function;
pub(crate) mod logical_function;

use self::{comparison_function::ComparisonFunction, logical_function::LogicalFunction};

/// Boolean expression.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum BooleanExpression {
    /// AND, OR, NOT
    LogicalFunctionVariant(LogicalFunction),

    /// Comparison functions
    ComparisonFunctionVariant(ComparisonFunction),
}
