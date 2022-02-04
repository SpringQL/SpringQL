pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use crate::{pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue};

use self::{boolean_expression::BooleanExpr, function_call::FunctionCall, operator::UnaryOperator};

pub(crate) trait ValueExpr {}

/// Value Expression (phase1).
///
/// A value expression (phase1) can be evaluated into SqlValue with a tuple (to resolve column reference).
///
/// ValueExprPh1 may contain column references to resolve from a row.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExprPh1 {
    Constant(SqlValue),
    UnaryOperator(UnaryOperator, Box<Self>),
    BooleanExpr(BooleanExpr<Self>),
    FunctionCall(FunctionCall),

    FieldPointer(FieldPointer),
}
impl ValueExpr for ValueExprPh1 {}

/// Value Expression (phase2).
///
/// A value expression phase2 can be evaluated by itself.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExprPh2 {
    Constant(SqlValue),
    UnaryOperator(UnaryOperator, Box<Self>),
    BooleanExpr(BooleanExpr<Self>),
    FunctionCall(FunctionCall),
}
impl ValueExpr for ValueExprPh2 {}
