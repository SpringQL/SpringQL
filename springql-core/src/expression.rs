pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use crate::{
    expression::boolean_expression::logical_function::LogicalFunction,
    pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue,
};

use self::{
    boolean_expression::{comparison_function::ComparisonFunction, BooleanExpression},
    function_call::FunctionCall,
    operator::UnaryOperator,
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

impl Expression {
    /// retrieves all FieldPointer in a expression
    pub fn to_field_pointers(&self) -> Vec<FieldPointer> {
        fn helper_boolean_expr(boolean_expr: &BooleanExpression) -> Vec<FieldPointer> {
            match boolean_expr {
                BooleanExpression::LogicalFunctionVariant(logical_function) => {
                    match logical_function {
                        LogicalFunction::AndVariant { left, right } => {
                            let mut left = helper_boolean_expr(&*left);
                            let mut right = helper_boolean_expr(&*right);
                            left.append(&mut right);
                            left
                        }
                    }
                }
                BooleanExpression::ComparisonFunctionVariant(comparison_function) => {
                    match comparison_function {
                        ComparisonFunction::EqualVariant { left, right } => {
                            let mut left = left.to_field_pointers();
                            let mut right = right.to_field_pointers();
                            left.append(&mut right);
                            left
                        }
                    }
                }
            }
        }

        fn helper_function_call(function_call: &FunctionCall) -> Vec<FieldPointer> {
            match function_call {
                FunctionCall::FloorTime {
                    target: expression, ..
                } => expression.to_field_pointers(),
            }
        }

        match self {
            Expression::Constant(_) => vec![],
            Expression::FieldPointer(idx) => vec![idx.clone()],
            Expression::UnaryOperator(_op, expr) => expr.to_field_pointers(),
            Expression::BooleanExpr(bool_expr) => helper_boolean_expr(bool_expr),
            Expression::FunctionCall(function_call) => helper_function_call(function_call),
        }
    }
}

impl From<SqlValue> for Expression {
    fn from(sql_val: SqlValue) -> Self {
        Self::Constant(sql_val)
    }
}
