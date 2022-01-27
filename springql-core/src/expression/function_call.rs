use super::Expression;

#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum FunctionCall {
    Floor { expression: Box<Expression> },
}
