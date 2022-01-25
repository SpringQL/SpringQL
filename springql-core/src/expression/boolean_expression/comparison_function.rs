use crate::expression::Expression;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ComparisonFunction {
    /// `=` operation
    EqualVariant {
        /// Left operand
        left: Box<Expression>,
        /// Right operand
        right: Box<Expression>,
    },
}
