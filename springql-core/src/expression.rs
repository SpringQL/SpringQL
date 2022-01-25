pub(crate) mod boolean_expression;
pub(crate) mod operator;

use crate::{
    expression::boolean_expression::logical_function::LogicalFunction,
    pipeline::field::field_pointer::FieldPointer, stream_engine::SqlValue,
};

use self::{
    boolean_expression::{comparison_function::ComparisonFunction, BooleanExpression},
    operator::UnaryOperator,
};

/// Expression.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum Expression {
    Constant(SqlValue),
    FieldPointer(FieldPointer),
    UnaryOperator(UnaryOperator, Box<Expression>),
    BooleanExpr(BooleanExpression),
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

        match self {
            Expression::Constant(_) => vec![],
            Expression::FieldPointer(idx) => vec![idx.clone()],
            Expression::UnaryOperator(_op, expr) => expr.to_field_pointers(),
            Expression::BooleanExpr(bool_expr) => helper_boolean_expr(bool_expr),
        }
    }
}

impl From<SqlValue> for Expression {
    fn from(sql_val: SqlValue) -> Self {
        Self::Constant(sql_val)
    }
}
