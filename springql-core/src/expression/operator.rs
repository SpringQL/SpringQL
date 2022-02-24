// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

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
