// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::expression::ValueExprType;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum ComparisonFunction<E>
where
    E: ValueExprType,
{
    /// `=` operation
    EqualVariant {
        /// Left operand
        left: Box<E>,
        /// Right operand
        right: Box<E>,
    },
}
