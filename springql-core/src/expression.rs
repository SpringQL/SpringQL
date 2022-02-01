pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use crate::{pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue};

use self::{
    boolean_expression::BooleanExpression, function_call::FunctionCall, operator::UnaryOperator,
};

/// Expression.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum Expression {
    Constant(SqlValue),
    FieldPointer(FieldPointer),
    UnaryOperator(UnaryOperator, Box<Expression>),
    BooleanExpr(BooleanExpression),
    FunctionCall(FunctionCall),
}

impl From<SqlValue> for Expression {
    fn from(sql_val: SqlValue) -> Self {
        Self::Constant(sql_val)
    }
}
