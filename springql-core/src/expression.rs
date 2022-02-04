pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use crate::{pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue};

use self::{
    boolean_expression::BooleanExpression, function_call::FunctionCall, operator::UnaryOperator,
};

/// Value Expression (phase1).
///
/// A value expression can be evaluated into SqlValue with a tuple (to resolve column reference).
///
/// ValueExpressionPh1 may contain column references to resolve from a row.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExprPh1 {
    Constant(SqlValue),
    FieldPointer(FieldPointer),
    UnaryOperator(UnaryOperator, Box<ValueExprPh1>),
    BooleanExpr(BooleanExpression),
    FunctionCall(FunctionCall),
}
