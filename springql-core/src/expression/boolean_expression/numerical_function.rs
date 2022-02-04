use crate::expression::ValueExprPh1;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum NumericalFunction {
    /// `+` operation
    AddVariant {
        left: Box<ValueExprPh1>,
        right: Box<ValueExprPh1>,
    },
}
