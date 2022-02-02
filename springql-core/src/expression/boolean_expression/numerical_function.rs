use crate::expression::Expression;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum NumericalFunction {
    /// `+` operation
    AddVariant {
        left: Box<Expression>,
        right: Box<Expression>,
    },
}
