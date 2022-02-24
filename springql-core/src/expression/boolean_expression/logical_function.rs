// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::expression::ValueExprType;

use super::BooleanExpr;

/// AND, OR, NOT
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum LogicalFunction<E>
where
    E: ValueExprType,
{
    /// `AND` operation
    AndVariant {
        /// Left operand
        left: Box<BooleanExpr<E>>,
        /// Right operand
        right: Box<BooleanExpr<E>>,
    },
}
