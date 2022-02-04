use crate::expression::ValueExpr;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ComparisonFunction<E>
where
    E: ValueExpr,
{
    /// `=` operation
    EqualVariant {
        /// Left operand
        left: Box<E>,
        /// Right operand
        right: Box<E>,
    },
}
