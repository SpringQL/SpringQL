//! Expression has two forms:
//!
//! 1. Value expression, which is evaluated into an SqlValue from a row.
//! 2. Aggregate expression, which is evaluated into an SqlValue from set of rows.
//!
//! Since SQL parser cannot distinguish column reference and value expression,
//! `ValueExprOrAlias` is used for value expressions excluding select_list.

pub(crate) mod boolean_expression;
pub(crate) mod function_call;
pub(crate) mod operator;

use anyhow::anyhow;

use crate::{
    error::{Result, SpringError},
    pipeline::{
        field::field_name::ColumnReference,
        pump_model::window_operation_parameter::aggregate::AggregateFunctionParameter,
    },
    stream_engine::{
        time::duration::{event_duration::EventDuration, SpringDuration},
        NnSqlValue, SqlCompareResult, SqlValue, Tuple,
    },
};

use self::{
    boolean_expression::{
        comparison_function::ComparisonFunction, logical_function::LogicalFunction,
        numerical_function::NumericalFunction, BooleanExpr,
    },
    function_call::FunctionCall,
    operator::UnaryOperator,
};

pub(crate) trait ValueExprType {}

/// Value Expression.
///
/// A value expression can be evaluated into SqlValue with a tuple (to resolve column reference).
/// ValueExpr may contain column references to resolve from a row.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExpr {
    Constant(SqlValue),
    UnaryOperator(UnaryOperator, Box<Self>),
    BooleanExpr(BooleanExpr<Self>),
    FunctionCall(FunctionCall<Self>),

    ColumnReference(ColumnReference),
}
impl ValueExprType for ValueExpr {}

impl ValueExpr {
    pub(crate) fn resolve_colref(self, tuple: &Tuple) -> Result<ValueExprPh2> {
        match self {
            Self::Constant(value) => Ok(ValueExprPh2::Constant(value)),

            Self::ColumnReference(colref) => {
                let value = tuple.get_value(&colref)?;
                Ok(ValueExprPh2::Constant(value))
            }

            Self::FunctionCall(function_call) => match function_call {
                FunctionCall::DurationMillis { duration_millis } => {
                    let duration_millis_ph2 = duration_millis.resolve_colref(tuple)?;
                    Ok(ValueExprPh2::FunctionCall(FunctionCall::DurationMillis {
                        duration_millis: Box::new(duration_millis_ph2),
                    }))
                }
                FunctionCall::DurationSecs { duration_secs } => {
                    let duration_secs_ph2 = duration_secs.resolve_colref(tuple)?;
                    Ok(ValueExprPh2::FunctionCall(FunctionCall::DurationSecs {
                        duration_secs: Box::new(duration_secs_ph2),
                    }))
                }
                FunctionCall::FloorTime { target, resolution } => {
                    let target_ph2 = target.resolve_colref(tuple)?;
                    let resolution_ph2 = resolution.resolve_colref(tuple)?;
                    Ok(ValueExprPh2::FunctionCall(FunctionCall::FloorTime {
                        target: Box::new(target_ph2),
                        resolution: Box::new(resolution_ph2),
                    }))
                }
            },
            Self::UnaryOperator(op, expr_ph1) => {
                let expr_ph2 = expr_ph1.resolve_colref(tuple)?;
                Ok(ValueExprPh2::UnaryOperator(op, Box::new(expr_ph2)))
            }
            Self::BooleanExpr(bool_expr) => match bool_expr {
                BooleanExpr::LogicalFunctionVariant(logical_function) => match logical_function {
                    LogicalFunction::AndVariant { left, right } => {
                        let left_ph2 = Self::BooleanExpr(*left).resolve_colref(tuple)?;
                        let right_ph2 = Self::BooleanExpr(*right).resolve_colref(tuple)?;
                        match (left_ph2, right_ph2) {
                            (
                                ValueExprPh2::BooleanExpr(left_ph2),
                                ValueExprPh2::BooleanExpr(right_ph2),
                            ) => Ok(ValueExprPh2::BooleanExpr(
                                BooleanExpr::LogicalFunctionVariant(LogicalFunction::AndVariant {
                                    left: Box::new(left_ph2),
                                    right: Box::new(right_ph2),
                                }),
                            )),
                            _ => unreachable!(),
                        }
                    }
                },
                BooleanExpr::ComparisonFunctionVariant(comparison_function) => {
                    match comparison_function {
                        ComparisonFunction::EqualVariant { left, right } => {
                            let left_ph2 = left.resolve_colref(tuple)?;
                            let right_ph2 = right.resolve_colref(tuple)?;
                            Ok(ValueExprPh2::BooleanExpr(
                                BooleanExpr::ComparisonFunctionVariant(
                                    ComparisonFunction::EqualVariant {
                                        left: Box::new(left_ph2),
                                        right: Box::new(right_ph2),
                                    },
                                ),
                            ))
                        }
                    }
                }
                BooleanExpr::NumericalFunctionVariant(numerical_function) => {
                    match numerical_function {
                        NumericalFunction::AddVariant { left, right } => {
                            let left_ph2 = left.resolve_colref(tuple)?;
                            let right_ph2 = right.resolve_colref(tuple)?;
                            Ok(ValueExprPh2::BooleanExpr(
                                BooleanExpr::NumericalFunctionVariant(
                                    NumericalFunction::AddVariant {
                                        left: Box::new(left_ph2),
                                        right: Box::new(right_ph2),
                                    },
                                ),
                            ))
                        }
                        NumericalFunction::MulVariant { left, right } => {
                            let left_ph2 = left.resolve_colref(tuple)?;
                            let right_ph2 = right.resolve_colref(tuple)?;
                            Ok(ValueExprPh2::BooleanExpr(
                                BooleanExpr::NumericalFunctionVariant(
                                    NumericalFunction::MulVariant {
                                        left: Box::new(left_ph2),
                                        right: Box::new(right_ph2),
                                    },
                                ),
                            ))
                        }
                    }
                }
            },
        }
    }
}

/// Value Expression (phase2).
///
/// A value expression phase2 can be evaluated by itself.
#[derive(Clone, PartialEq, Hash, Debug)]
pub(crate) enum ValueExprPh2 {
    Constant(SqlValue),
    UnaryOperator(UnaryOperator, Box<Self>),
    BooleanExpr(BooleanExpr<Self>),
    FunctionCall(FunctionCall<Self>),
}
impl ValueExprType for ValueExprPh2 {}

impl ValueExprPh2 {
    pub(crate) fn eval(self) -> Result<SqlValue> {
        match self {
            Self::Constant(sql_value) => Ok(sql_value),
            Self::UnaryOperator(uni_op, child) => {
                let child_sql_value = child.eval()?;
                match (uni_op, child_sql_value) {
                    (UnaryOperator::Minus, SqlValue::Null) => Ok(SqlValue::Null),
                    (UnaryOperator::Minus, SqlValue::NotNull(nn_sql_value)) => {
                        Ok(SqlValue::NotNull(nn_sql_value.negate()?))
                    }
                }
            }
            Self::BooleanExpr(bool_expr) => match bool_expr {
                BooleanExpr::ComparisonFunctionVariant(comparison_function) => {
                    match comparison_function {
                        ComparisonFunction::EqualVariant { left, right } => {
                            let left_sql_value = left.eval()?;
                            let right_sql_value = right.eval()?;
                            left_sql_value
                                .sql_compare(&right_sql_value)
                                .map(|sql_compare_result| {
                                    SqlValue::NotNull(NnSqlValue::Boolean(matches!(
                                        sql_compare_result,
                                        SqlCompareResult::Eq
                                    )))
                                })
                        }
                    }
                }
                BooleanExpr::LogicalFunctionVariant(logical_function) => match logical_function {
                    LogicalFunction::AndVariant { left, right } => {
                        let left_sql_value = Self::BooleanExpr(*left).eval()?;
                        let right_sql_value = Self::BooleanExpr(*right).eval()?;

                        let b = left_sql_value.to_bool()? && right_sql_value.to_bool()?;
                        Ok(SqlValue::NotNull(NnSqlValue::Boolean(b)))
                    }
                },
                BooleanExpr::NumericalFunctionVariant(numerical_function) => {
                    Self::eval_numerical_function(numerical_function)
                }
            },
            Self::FunctionCall(function_call) => Self::eval_function_call(function_call),
        }
    }
    fn eval_numerical_function(numerical_function: NumericalFunction<Self>) -> Result<SqlValue> {
        match numerical_function {
            NumericalFunction::AddVariant { left, right } => {
                let left_sql_value = left.eval()?;
                let right_sql_value = right.eval()?;
                left_sql_value + right_sql_value
            }
            NumericalFunction::MulVariant { left, right } => {
                let left_sql_value = left.eval()?;
                let right_sql_value = right.eval()?;
                left_sql_value * right_sql_value
            }
        }
    }

    fn eval_function_call(function_call: FunctionCall<Self>) -> Result<SqlValue> {
        match function_call {
            FunctionCall::FloorTime { target, resolution } => {
                Self::eval_function_floor_time(*target, *resolution)
            }
            FunctionCall::DurationMillis { duration_millis } => {
                Self::eval_function_duration_millis(*duration_millis)
            }
            FunctionCall::DurationSecs { duration_secs } => {
                Self::eval_function_duration_secs(*duration_secs)
            }
        }
    }

    fn eval_function_floor_time(target: Self, resolution: Self) -> Result<SqlValue> {
        let target_value = target.eval()?;
        let resolution_value = resolution.eval()?;

        match (&target_value, &resolution_value) {
            (
                SqlValue::NotNull(NnSqlValue::Timestamp(ts)),
                SqlValue::NotNull(NnSqlValue::Duration(resolution)),
            ) => {
                let ts_floor = ts.floor(resolution.to_chrono());
                Ok(SqlValue::NotNull(NnSqlValue::Timestamp(ts_floor)))
            }
            _ => Err(SpringError::Sql(anyhow!(
                "invalid parameter to FLOOR_TIME: `({}, {})`",
                target_value,
                resolution_value
            ))),
        }
    }

    fn eval_function_duration_millis(duration_millis: Self) -> Result<SqlValue> {
        let duration_value = duration_millis.eval()?;
        let duration_millis = duration_value.to_i64()?;
        if duration_millis >= 0 {
            let duration = EventDuration::from_millis(duration_millis as u64);
            Ok(SqlValue::NotNull(NnSqlValue::Duration(duration)))
        } else {
            Err(SpringError::Sql(anyhow!(
                "DURATION_MILLIS should take positive integer but got `{}`",
                duration_millis
            )))
        }
    }
    fn eval_function_duration_secs(duration_secs: Self) -> Result<SqlValue> {
        let duration_value = duration_secs.eval()?;
        let duration_secs = duration_value.to_i64()?;
        if duration_secs >= 0 {
            let duration = EventDuration::from_secs(duration_secs as u64);
            Ok(SqlValue::NotNull(NnSqlValue::Duration(duration)))
        } else {
            Err(SpringError::Sql(anyhow!(
                "DURATION_SECS should take positive integer but got `{}`",
                duration_secs
            )))
        }
    }
}

/// Aggregate expression.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct AggrExpr {
    pub(crate) func: AggregateFunctionParameter,
    pub(crate) aggregated: ValueExpr,
}
