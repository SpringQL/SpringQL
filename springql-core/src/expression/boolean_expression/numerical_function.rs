// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::expression::ValueExprType;

#[derive(Clone, PartialEq, Hash, Debug)]
pub enum NumericalFunction<E>
where
    E: ValueExprType,
{
    /// `+` operation
    AddVariant { left: Box<E>, right: Box<E> },

    /// `*` operation
    MulVariant { left: Box<E>, right: Box<E> },
}
