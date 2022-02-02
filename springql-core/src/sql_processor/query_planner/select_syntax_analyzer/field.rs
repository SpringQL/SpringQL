use super::SelectSyntaxAnalyzer;
use crate::{
    error::{Result, SpringError},
    expression::{function_call::FunctionCall, Expression},
    pipeline::{
        field::{field_name::ColumnReference, field_pointer::FieldPointer},
        name::{AttributeName, ColumnName, StreamName},
    },
    sql_processor::sql_parser::syntax::SelectFieldSyntax,
};
use anyhow::anyhow;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn field_expressions(&self) -> Vec<Expression> {
        let select_fields = &self.select_syntax.fields;
        select_fields
            .into_iter()
            .map(|field| &field.expression)
            .cloned()
            .collect()
    }

    pub(in super::super) fn column_references_in_projection(&self) -> Result<Vec<ColumnReference>> {
        let from_item_correlations = self.from_item_streams()?;
        let select_fields = &self.select_syntax.fields;

        select_fields
            .iter()
            .map(|select_field| {
                Self::select_field_into_colref(select_field, &from_item_correlations)
            })
            .collect::<Result<_>>()
    }

    fn select_field_into_colref(
        select_field: &SelectFieldSyntax,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        match &select_field.expression {
            Expression::Constant(_) => {
                unimplemented!("constant in select field is not supported currently",)
            }
            Expression::UnaryOperator(_, _) | Expression::BooleanExpr(_) => {
                // TODO Better to shrink expression in this layer.
                unimplemented!("unary/binary operation in select field is not supported currently",)
            }
            Expression::FieldPointer(ptr) => Self::column_reference(ptr, from_item_streams),
            Expression::FunctionCall(fun_call) => match fun_call {
                FunctionCall::FloorTime { target, .. } => {
                    // TODO will use label for projection
                    match target.as_ref() {
                        Expression::FieldPointer(ptr) => Ok(ColumnReference::new(
                            from_item_streams.first().unwrap().clone(), // super ugly...
                            ColumnName::new(ptr.attr().to_string()),
                        )),
                        _ => unimplemented!(),
                    }
                }
                FunctionCall::DurationSecs { .. } => {
                    unreachable!("DURATION_SECS() cannot appear in field list")
                }
            },
        }
    }

    /// TODO may need Pipeline when:
    /// - pointer does not have prefix part and
    /// - from_item_correlations are more than 1
    /// because this function has to determine which of `from1` or `from2` `field1` is from.
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - none of `from_item_correlations` has field named `pointer.column_name`
    ///   - `pointer` has a correlation but it is not any of `from_item_correlations`.
    pub(super) fn column_reference(
        pointer: &FieldPointer,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        if from_item_streams.is_empty() {
            unreachable!("SQL parser must handle this case")
        } else if let Some(corr) = pointer.prefix() {
            Self::field_name_with_prefix(corr, pointer.attr(), from_item_streams)
        } else {
            Self::field_name_without_prefix(pointer.attr(), from_item_streams)
        }
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - `prefix` does not match any of `from_item_correlations`.
    fn field_name_with_prefix(
        prefix: &str,
        attr: &str,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        assert!(!from_item_streams.is_empty());

        let attr = AttributeName::new(attr.to_string());
        let pointer = FieldPointer::from(format!("{}.{}", prefix, attr).as_str());

        // SELECT T.C FROM ...;
        from_item_streams
            .iter()
            .find_map(|stream_name| {
                // creates ColumnReference to use .matches()
                let colref_candidate =
                    ColumnReference::new(stream_name.clone(), ColumnName::new(attr.to_string()));
                colref_candidate.matches(&pointer).then(|| colref_candidate)
            })
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    "`{}` does not match any of FROM items: {:?}",
                    pointer,
                    from_item_streams
                ))
            })
    }

    fn field_name_without_prefix(
        attr: &str,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        assert!(!from_item_streams.is_empty());
        if from_item_streams.len() > 1 {
            return Err(SpringError::Sql(anyhow!(
                "needs pipeline info to detect which stream has the column `{:?}`",
                attr
            )));
        }

        // SELECT C FROM T (AS a)?;
        // -> C is from T
        let from_item_stream = from_item_streams[0].clone();
        let attr = AttributeName::new(attr.to_string());
        Ok(ColumnReference::new(
            from_item_stream,
            ColumnName::new(attr.to_string()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[derive(new)]
    struct TestDatum {
        index: FieldPointer,
        from_item_streams: Vec<StreamName>,
        expected_result: Result<ColumnReference>,
    }

    #[test]
    fn test_field_pointer() {
        let test_data: Vec<TestDatum> = vec![
            TestDatum::new(
                FieldPointer::from("c"),
                vec![StreamName::factory("t")],
                Ok(ColumnReference::factory("t", "c")),
            ),
            TestDatum::new(
                FieldPointer::from("t.c"),
                vec![StreamName::factory("t")],
                Ok(ColumnReference::factory("t", "c")),
            ),
            TestDatum::new(
                FieldPointer::from("t1.c"),
                vec![StreamName::factory("t2")],
                Err(SpringError::Sql(anyhow!(""))),
            ),
            TestDatum::new(
                FieldPointer::from("c"),
                vec![StreamName::factory("t")],
                Ok(ColumnReference::factory("t", "c")),
            ),
            TestDatum::new(
                FieldPointer::from("t.c"),
                vec![StreamName::factory("t")],
                Ok(ColumnReference::factory("t", "c")),
            ),
            TestDatum::new(
                FieldPointer::from("x.c"),
                vec![StreamName::factory("t")],
                Err(SpringError::Sql(anyhow!(""))),
            ),
        ];

        for test_datum in test_data {
            match SelectSyntaxAnalyzer::column_reference(
                &test_datum.index,
                &test_datum.from_item_streams,
            ) {
                Ok(field_name) => {
                    assert_eq!(field_name, test_datum.expected_result.unwrap())
                }
                Err(e) => {
                    assert!(matches!(e, SpringError::Sql(_)))
                }
            }
        }
    }
}
