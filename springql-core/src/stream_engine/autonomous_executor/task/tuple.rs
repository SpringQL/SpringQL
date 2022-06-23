// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::error::{Result, SpringError},
    mem_size::MemSize,
    pipeline::{ColumnReference, Field},
    stream_engine::{
        autonomous_executor::row::{RowTime, StreamRow},
        NnSqlValue, SqlValue,
    },
};
use anyhow::anyhow;

/// Tuple is a temporary structure appearing only in task execution.
///
/// 1. Task gets a row from input queue.
/// 2. Task converts the row into tuple.
/// 3. Task puts a row converted from the final tuple (for column values) and ExprResolver (for expressions).
///
/// Unlike rows, tuples may have not only stream's columns but also fields derived from expressions.
#[derive(Clone, PartialEq, Debug, new)]
pub struct Tuple {
    /// If this tuple is constructed from has a ROWTIME column, `rowtime` has duplicate value with one of `fields`.
    rowtime: RowTime,
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
    pub fn from_row(row: StreamRow) -> Self {
        let rowtime = row.rowtime();

        let stream_name = row.stream_model().name().clone();
        let fields = row
            .into_iter()
            .map(|(column_name, sql_value)| {
                let colref = ColumnReference::Column {
                    stream_name: stream_name.clone(),
                    column_name,
                };
                Field::new(colref, sql_value)
            })
            .collect();

        Self { rowtime, fields }
    }

    pub fn rowtime(&self) -> RowTime {
        self.rowtime
    }

    /// # Failures
    ///
    /// `SpringError::Sql` when:
    ///   - `column_reference` does not match any field.
    ///   - processing time is referenced while event time is defined to the stream.
    pub fn get_value(&self, column_reference: &ColumnReference) -> Result<SqlValue> {
        match column_reference {
            ColumnReference::Column { .. } => self.get_column_value(column_reference),
            ColumnReference::PTime { .. } => self.get_processing_time(),
        }
    }

    /// # Failures
    ///
    /// `SpringError::Sql` if `column_reference` does not match any field.
    pub fn get_column_value(&self, column_reference: &ColumnReference) -> Result<SqlValue> {
        let sql_value = self.fields.iter().find_map(|field| {
            let colref = field.name();
            (colref == column_reference).then(|| field.sql_value().clone())
        });

        sql_value
            .ok_or_else(|| SpringError::Sql(anyhow!("cannot find field `{:?}`", column_reference)))
    }

    /// # Failures
    ///
    /// `SpringError::Sql` if `column_reference` does not match any field.
    pub fn get_processing_time(&self) -> Result<SqlValue> {
        if let RowTime::ProcessingTime(ptime) = self.rowtime {
            Ok(SqlValue::NotNull(NnSqlValue::Timestamp(ptime)))
        } else {
            Err(SpringError::Sql(anyhow!(
                "processing time is not available for this stream"
            )))
        }
    }

    /// Left rowtime is used for joined tuple.
    pub fn join(self, right: Self) -> Tuple {
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
    use crate::{
        expression::{UnaryOperator, ValueExpr},
        stream_engine::time::SpringTimestamp,
    };

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
                Tuple::factory_trade(SpringTimestamp::fx_ts1(), "ORCL", 1),
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
