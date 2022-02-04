use crate::expression::ValueExpr;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ComparisonFunction {
    /// `=` operation
    EqualVariant {
        /// Left operand
        left: Box<ValueExpr>,
        /// Right operand
        right: Box<ValueExpr>,
    },
}
