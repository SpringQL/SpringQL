// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

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

    /// *
    Mul,

    /// AND
    And,
}
