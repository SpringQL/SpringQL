use crate::{
    error::{Result, SpringError},
    expression::{
        boolean_expression::{
            comparison_function::ComparisonFunction, logical_function::LogicalFunction,
            BooleanExpression,
        },
        function_call::FunctionCall,
        operator::UnaryOperator,
        Expression,
    },
    pipeline::{
        field::{field_name::ColumnReference, field_pointer::FieldPointer, Field},
        name::ColumnName,
        stream_model::StreamModel,
    },
    stream_engine::{
        autonomous_executor::row::{
            column::stream_column::StreamColumns,
            column_values::ColumnValues,
            value::sql_value::{nn_sql_value::NnSqlValue, sql_compare_result::SqlCompareResult},
            Row,
        },
        time::timestamp::Timestamp,
        SqlValue,
    },
};
use anyhow::anyhow;
use chrono::Duration;
use std::sync::Arc;

/// Tuple is a temporary structure appearing only in task execution.
///
/// 1. Task gets a row from input queue.
/// 2. Task converts the row into tuple.
/// 3. Task puts a row converted from the final tuple.
///
/// Unlike rows, tuples may have not only stream's columns but also fields derived from expressions.
#[derive(Clone, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct Tuple {
    /// Either be an event-time or a process-time.
    /// If a row this tuple is constructed from has a ROWTIME column, `rowtime` has duplicate value with one of `fields`.
    rowtime: Timestamp,

    fields: Vec<Field>,
}

impl Tuple {
    pub(in crate::stream_engine::autonomous_executor) fn from_row(row: Row) -> Self {
        let rowtime = row.rowtime();

        let stream_name = row.stream_model().name().clone();
        let fields = row
            .into_iter()
            .map(|(column_name, sql_value)| {
                let colref = ColumnReference::new(stream_name.clone(), column_name);
                Field::new(colref, sql_value)
            })
            .collect();

        Self { rowtime, fields }
    }

    /// ```text
    /// column_order = (c2, c3, c1)
    /// stream_shape = (c1, c2, c3)
    ///
    /// |
    /// v
    ///
    /// (fields[1], fields[2], fields[0])
    /// ```
    ///
    /// # Panics
    ///
    /// - Tuple fields and column_order have different length.
    /// - Type mismatch between `self.fields` (ordered) and `stream_shape`
    /// - Duplicate column names in `column_order`
    pub(super) fn into_row(
        self,
        stream_model: Arc<StreamModel>,
        column_order: Vec<ColumnName>,
    ) -> Row {
        assert_eq!(self.fields.len(), column_order.len());

        let column_values = self.mk_column_values(column_order);
        let stream_columns = StreamColumns::new(stream_model, column_values)
            .expect("type or shape mismatch? must be checked on pump creation");
        Row::new(stream_columns)
    }

    pub(in crate::stream_engine::autonomous_executor) fn rowtime(&self) -> &Timestamp {
        &self.rowtime
    }

    /// # Failures
    ///
    /// `SpringError::Sql` if `field_pointer` does not match any field.
    pub(super) fn get_value(&self, field_pointer: &FieldPointer) -> Result<SqlValue> {
        let sql_value = self.fields.iter().find_map(|field| {
            let colrefs = field.name();
            colrefs
                .matches(field_pointer)
                .then(|| field.sql_value().clone())
        });

        sql_value.ok_or_else(|| SpringError::Sql(anyhow!("cannot find field `{}`", field_pointer)))
    }

    pub(super) fn eval_expression(&self, expr: Expression) -> Result<SqlValue> {
        match expr {
            Expression::Constant(sql_value) => Ok(sql_value),
            Expression::FieldPointer(ptr) => self.get_value(&ptr),
            Expression::UnaryOperator(uni_op, child) => {
                let child_sql_value = self.eval_expression(*child)?;
                match (uni_op, child_sql_value) {
                    (UnaryOperator::Minus, SqlValue::Null) => Ok(SqlValue::Null),
                    (UnaryOperator::Minus, SqlValue::NotNull(nn_sql_value)) => {
                        Ok(SqlValue::NotNull(nn_sql_value.negate()?))
                    }
                }
            }
            Expression::BooleanExpr(bool_expr) => match bool_expr {
                BooleanExpression::ComparisonFunctionVariant(comparison_function) => {
                    match comparison_function {
                        ComparisonFunction::EqualVariant { left, right } => {
                            let left_sql_value = self.eval_expression(*left)?;
                            let right_sql_value = self.eval_expression(*right)?;
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
                BooleanExpression::LogicalFunctionVariant(logical_function) => {
                    match logical_function {
                        LogicalFunction::AndVariant { left, right } => {
                            let left_sql_value =
                                self.eval_expression(Expression::BooleanExpr(*left))?;
                            let right_sql_value =
                                self.eval_expression(Expression::BooleanExpr(*right))?;

                            let b = left_sql_value.to_bool()? && right_sql_value.to_bool()?;
                            Ok(SqlValue::NotNull(NnSqlValue::Boolean(b)))
                        }
                    }
                }
            },
            Expression::FunctionCall(function_call) => self.eval_function_call(function_call),
        }
    }

    fn eval_function_call(&self, function_call: FunctionCall) -> Result<SqlValue> {
        match function_call {
            FunctionCall::FloorTime { target: expression } => {
                let sql_value = self.eval_expression(*expression)?;
                match sql_value {
                    SqlValue::NotNull(NnSqlValue::Timestamp(ts)) => {
                        // TODO resolution from syntax
                        let ts_floor = ts.floor(Duration::seconds(10));
                        Ok(SqlValue::NotNull(NnSqlValue::Timestamp(ts_floor)))
                    }
                    _ => Err(SpringError::Sql(anyhow!(
                        "floor is only supported for non-NULL TIMESTAMP `{}`",
                        sql_value
                    ))),
                }
            }
        }
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - No field named `field_pointer` is found from this tuple.
    pub(super) fn projection(self, field_pointers: &[FieldPointer]) -> Result<Self> {
        let mut fields = self.fields.into_iter();

        let new_fields = field_pointers
            .iter()
            .map(|pointer| {
                fields
                    .find(|field| field.name().matches(pointer))
                    .ok_or_else(|| {
                        SpringError::Sql(anyhow!("cannot find field `{}` in tuple", pointer))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            rowtime: self.rowtime,
            fields: new_fields,
        })
    }

    fn mk_column_values(self, column_order: Vec<ColumnName>) -> ColumnValues {
        let mut column_values = ColumnValues::default();

        for (column_name, field) in column_order.into_iter().zip(self.fields.into_iter()) {
            column_values
                .insert(column_name, field.into_sql_value())
                .expect("duplicate column name");
        }

        column_values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_sql_value() {
        #[derive(new)]
        struct TestDatum {
            in_expr: Expression,
            tuple: Tuple,
            expected_sql_value: SqlValue,
        }

        let test_data: Vec<TestDatum> = vec![
            // constants
            TestDatum::new(
                Expression::factory_integer(1),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_integer(1),
            ),
            // unary op
            TestDatum::new(
                Expression::factory_uni_op(UnaryOperator::Minus, Expression::factory_integer(1)),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_integer(-1),
            ),
            // FieldPointer
            TestDatum::new(
                Expression::FieldPointer(FieldPointer::from("amount")),
                Tuple::factory_trade(Timestamp::fx_ts1(), "ORCL", 1),
                SqlValue::factory_integer(1),
            ),
            // BooleanExpression
            TestDatum::new(
                Expression::factory_eq(Expression::factory_null(), Expression::factory_null()),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
            TestDatum::new(
                Expression::factory_eq(
                    Expression::factory_integer(123),
                    Expression::factory_integer(123),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(true),
            ),
            TestDatum::new(
                Expression::factory_eq(
                    Expression::factory_integer(123),
                    Expression::factory_integer(-123),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
            TestDatum::new(
                Expression::factory_and(
                    BooleanExpression::factory_eq(
                        Expression::factory_integer(123),
                        Expression::factory_integer(123),
                    ),
                    BooleanExpression::factory_eq(
                        Expression::factory_integer(456),
                        Expression::factory_integer(456),
                    ),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(true),
            ),
            TestDatum::new(
                Expression::factory_and(
                    BooleanExpression::factory_eq(
                        Expression::factory_integer(-123),
                        Expression::factory_integer(123),
                    ),
                    BooleanExpression::factory_eq(
                        Expression::factory_integer(456),
                        Expression::factory_integer(456),
                    ),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
        ];

        for t in test_data {
            let sql_value = t.tuple.eval_expression(t.in_expr).unwrap();
            assert_eq!(sql_value, t.expected_sql_value);
        }
    }
}
