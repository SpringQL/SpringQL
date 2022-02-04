use crate::expression::ValueExprPh1;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ComparisonFunction {
    /// `=` operation
    EqualVariant {
        /// Left operand
        left: Box<ValueExprPh1>,
        /// Right operand
        right: Box<ValueExprPh1>,
    },
}
