// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::expression::ValueExprType;

/// Comparison function and its operands
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ComparisonFunction<E>
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
