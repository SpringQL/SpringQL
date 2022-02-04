use crate::expression::ValueExpr;

use super::BooleanExpr;

/// AND, OR, NOT
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum LogicalFunction<E>
where
    E: ValueExpr,
{
    /// `AND` operation
    AndVariant {
        /// Left operand
        left: Box<BooleanExpr<E>>,
        /// Right operand
        right: Box<BooleanExpr<E>>,
    },
}
