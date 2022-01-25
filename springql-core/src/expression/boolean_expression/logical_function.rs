use super::BooleanExpression;

/// AND, OR, NOT
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum LogicalFunction {
    /// `AND` operation
    AndVariant {
        /// Left operand
        left: Box<BooleanExpression>,
        /// Right operand
        right: Box<BooleanExpression>,
    },
}
