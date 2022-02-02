/// unary operator for an expression
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum UnaryOperator {
    /// -
    Minus,
}

/// binary operator for an expression
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum BinaryOperator {
    /// =
    Equal,

    /// +
    Add,
}
