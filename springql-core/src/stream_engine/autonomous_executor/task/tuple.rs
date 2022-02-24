// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    error::{Result, SpringError},
    mem_size::MemSize,
    pipeline::field::{field_name::ColumnReference, Field},
    stream_engine::{autonomous_executor::row::Row, time::timestamp::Timestamp, SqlValue},
};
use anyhow::anyhow;

/// Tuple is a temporary structure appearing only in task execution.
///
/// 1. Task gets a row from input queue.
/// 2. Task converts the row into tuple.
/// 3. Task puts a row converted from the final tuple.
///
/// Unlike rows, tuples may have not only stream's columns but also fields derived from expressions.
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct Tuple {
    /// Either be an event-time or a process-time.
    /// If a row this tuple is constructed from has a ROWTIME column, `rowtime` has duplicate value with one of `fields`.
    rowtime: Timestamp,

    fields: Vec<Field>,
}

impl MemSize for Tuple {
    fn mem_size(&self) -> usize {
        let rowtime_size = self.rowtime.mem_size();
        let fields_size: usize = self.fields.iter().map(|f| f.mem_size()).sum();
        rowtime_size + fields_size
    }
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

    pub(in crate::stream_engine::autonomous_executor) fn rowtime(&self) -> &Timestamp {
        &self.rowtime
    }

    /// # Failures
    ///
    /// `SpringError::Sql` if `column_reference` does not match any field.
    pub(crate) fn get_value(&self, column_reference: &ColumnReference) -> Result<SqlValue> {
        let sql_value = self.fields.iter().find_map(|field| {
            let colref = field.name();
            (colref == column_reference).then(|| field.sql_value().clone())
        });

        sql_value
            .ok_or_else(|| SpringError::Sql(anyhow!("cannot find field `{:?}`", column_reference)))
    }

    /// Left rowtime is used for joined tuple.
    pub(in crate::stream_engine::autonomous_executor) fn join(self, right: Self) -> Tuple {
        let rowtime = self.rowtime;

        let mut new_fields = self.fields;
        new_fields.extend(right.fields);

        Self {
            rowtime,
            fields: new_fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::{operator::UnaryOperator, ValueExpr};

    use super::*;

    #[test]
    fn test_to_sql_value() {
        #[derive(new)]
        struct TestDatum {
            in_expr: ValueExpr,
            tuple: Tuple,
            expected_sql_value: SqlValue,
        }

        let test_data: Vec<TestDatum> = vec![
            // constants
            TestDatum::new(
                ValueExpr::factory_integer(1),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_integer(1),
            ),
            // unary op
            TestDatum::new(
                ValueExpr::factory_uni_op(UnaryOperator::Minus, ValueExpr::factory_integer(1)),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_integer(-1),
            ),
            // ColumnReference
            TestDatum::new(
                ValueExpr::ColumnReference(ColumnReference::factory("trade", "amount")),
                Tuple::factory_trade(Timestamp::fx_ts1(), "ORCL", 1),
                SqlValue::factory_integer(1),
            ),
            // BooleanExpression
            TestDatum::new(
                ValueExpr::factory_eq(ValueExpr::factory_null(), ValueExpr::factory_null()),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
            TestDatum::new(
                ValueExpr::factory_eq(
                    ValueExpr::factory_integer(123),
                    ValueExpr::factory_integer(123),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(true),
            ),
            TestDatum::new(
                ValueExpr::factory_eq(
                    ValueExpr::factory_integer(123),
                    ValueExpr::factory_integer(-123),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
            TestDatum::new(
                ValueExpr::factory_and(
                    ValueExpr::factory_eq(
                        ValueExpr::factory_integer(123),
                        ValueExpr::factory_integer(123),
                    ),
                    ValueExpr::factory_eq(
                        ValueExpr::factory_integer(456),
                        ValueExpr::factory_integer(456),
                    ),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(true),
            ),
            TestDatum::new(
                ValueExpr::factory_and(
                    ValueExpr::factory_eq(
                        ValueExpr::factory_integer(-123),
                        ValueExpr::factory_integer(123),
                    ),
                    ValueExpr::factory_eq(
                        ValueExpr::factory_integer(456),
                        ValueExpr::factory_integer(456),
                    ),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(false),
            ),
        ];

        for t in test_data {
            let expr_ph2 = t.in_expr.resolve_colref(&t.tuple).unwrap();
            let sql_value = expr_ph2.eval().unwrap();
            assert_eq!(sql_value, t.expected_sql_value);
        }
    }
}
