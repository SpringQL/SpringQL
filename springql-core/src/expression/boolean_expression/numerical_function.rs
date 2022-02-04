use crate::expression::ValueExpr;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum NumericalFunction {
    /// `+` operation
    AddVariant {
        left: Box<ValueExpr>,
        right: Box<ValueExpr>,
    },
}
