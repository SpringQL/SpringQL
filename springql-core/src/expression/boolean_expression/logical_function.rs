// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::expression::ValueExprType;

/// AND, OR, NOT
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum LogicalFunction<E>
where
    E: ValueExprType,
{
    /// `AND` operation
    AndVariant {
        /// Left operand
        left: Box<E>,
        /// Right operand
        right: Box<E>,
    },
}
