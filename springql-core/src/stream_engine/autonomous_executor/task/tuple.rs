use crate::{
    error::{Result, SpringError},
    pipeline::{
        field::{field_name::ColumnReference, Field},
        name::ColumnName,
        stream_model::StreamModel,
    },
    stream_engine::{
        autonomous_executor::row::{
            column::stream_column::StreamColumns, column_values::ColumnValues, Row,
        },
        time::timestamp::Timestamp,
        SqlValue,
    },
};
use anyhow::anyhow;
use std::sync::Arc;

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
    /// `SpringError::Sql` if `column_reference` does not match any field.
    pub(crate) fn get_value(&self, column_reference: &ColumnReference) -> Result<SqlValue> {
        let sql_value = self.fields.iter().find_map(|field| {
            let colref = field.name();
            (colref == column_reference).then(|| field.sql_value().clone())
        });

        sql_value
            .ok_or_else(|| SpringError::Sql(anyhow!("cannot find field `{:?}`", column_reference)))
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - No field named `colrefs` is found from this tuple.
    pub(super) fn projection(self, colrefs: &[ColumnReference]) -> Result<Self> {
        let mut fields = self.fields.into_iter();

        let new_fields = colrefs
            .iter()
            .map(|colref| {
                fields.find(|field| field.name() == colref).ok_or_else(|| {
                    SpringError::Sql(anyhow!("cannot find field `{:?}` in tuple", colref))
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
    use crate::expression::{
        boolean_expression::BooleanExpr, operator::UnaryOperator, ValueExpr,
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
                ValueExpr::factory_uni_op(
                    UnaryOperator::Minus,
                    ValueExpr::factory_integer(1),
                ),
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
                ValueExpr::factory_eq(
                    ValueExpr::factory_null(),
                    ValueExpr::factory_null(),
                ),
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
                    BooleanExpr::factory_eq(
                        ValueExpr::factory_integer(123),
                        ValueExpr::factory_integer(123),
                    ),
                    BooleanExpr::factory_eq(
                        ValueExpr::factory_integer(456),
                        ValueExpr::factory_integer(456),
                    ),
                ),
                Tuple::fx_trade_oracle(),
                SqlValue::factory_bool(true),
            ),
            TestDatum::new(
                ValueExpr::factory_and(
                    BooleanExpr::factory_eq(
                        ValueExpr::factory_integer(-123),
                        ValueExpr::factory_integer(123),
                    ),
                    BooleanExpr::factory_eq(
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
