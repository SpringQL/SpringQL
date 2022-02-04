use crate::expression::ValueExpr;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum NumericalFunction<E>
where
    E: ValueExpr,
{
    /// `+` operation
    AddVariant { left: Box<E>, right: Box<E> },
}
