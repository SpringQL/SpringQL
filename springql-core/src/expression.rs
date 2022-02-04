pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use crate::{pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue};

use self::{
    boolean_expression::BooleanExpression, function_call::FunctionCall, operator::UnaryOperator,
};

/// Value Expression.
///
/// A value expression can be evaluated into SqlValue with a tuple (to resolve column reference).
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExpr {
    Constant(SqlValue),
    FieldPointer(FieldPointer),
    UnaryOperator(UnaryOperator, Box<ValueExpr>),
    BooleanExpr(BooleanExpression),
    FunctionCall(FunctionCall),
}

impl From<SqlValue> for ValueExpr {
    fn from(sql_val: SqlValue) -> Self {
        Self::Constant(sql_val)
    }
}
