use crate::expression::ValueExprType;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum NumericalFunction<E>
where
    E: ValueExprType,
{
    /// `+` operation
    AddVariant { left: Box<E>, right: Box<E> },

    /// `*` operation
    MulVariant { left: Box<E>, right: Box<E> },
}
